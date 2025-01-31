import base64
import traceback
import re

import httpx

from PLATER.services.config import config
from PLATER.services.util.drivers.neo4j_driver import Neo4jHTTPDriver
from PLATER.services.util.drivers.redis_driver import RedisDriver
from PLATER.services.util.logutil import LoggingUtil
from bmt import Toolkit


logger = LoggingUtil.init_logging(__name__,
                                  config.get('logging_level'),
                                  config.get('logging_format')
                                  )

class GraphInterface:
    """
    Singleton class for interfacing with the graph.
    """

    class _GraphInterface:
        def __init__(self, host, port, auth, backend='neo4j', db_name=None):
            if backend == 'neo4j':
                self.driver = Neo4jHTTPDriver(host=host, port=port, auth=auth)
            elif backend == 'redis':
                self.driver = RedisDriver(host=host, port=port, password=auth[1], graph_db_name=db_name)
            self.schema = None
            self.summary = None
            self.toolkit = Toolkit()

        def find_biolink_leaves(self, biolink_concepts: list):
            """
            Given a list of biolink concepts, returns the leaves removing any parent concepts.
            :param biolink_concepts: list of biolink concepts
            :return: leave concepts.
            """
            ancestry_set = set()
            all_concepts = set(biolink_concepts)
            for x in all_concepts:
                ancestors = set(self.toolkit.get_ancestors(x, reflexive=False, formatted=True))
                ancestry_set = ancestry_set.union(ancestors)
            leaf_set = all_concepts - ancestry_set
            return leaf_set

        def search(self, query, indexes, fields=None, options={
            "prefix_search": False,
            "postprocessing_cypher": "",
            "levenshtein_distance": 0,
            "query_limit": 50,
        }):
            """
            Execute a query against the graph's RediSearch indexes
            :param query: Search query.
            :type query: str
            :param indexes: List of indexes to search against.
            :type indexes: list
            :param [fields]: List of properties to search against. If none, searches all fields. Note that this argument is unimplemneted and will be ignored.
            :type [fields]: list
            :param [options]: Additional configuration options specifying how the search should be executed against the graph.
            :type [options]: dict
            :return: List of nodes and search scores
            :rtype: List[dict]
            """
            prefix_search = options.get("prefix_search", False)
            postprocessing_cypher = options.get("postprocessing_cypher", "")
            levenshtein_distance = options.get("levenshtein_distance", 0)
            query_limit = options.get("query_limit", 50)
            # It seems that stop words and token characters don't tokenize properly and simply break within
            # redisgraph's current RediSearch implementation (https://github.com/RedisGraph/RedisGraph/issues/1638)
            stop_words = [
                'a', 'is', 'the', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by', 'for', 'if', 'in', 'into', 'it',
                'no', 'not', 'of', 'on', 'or', 'such', 'that', 'their', 'then', 'there', 'these', 'they', 'this', 'to',
                'was', 'will', 'with'
            ]
            token_chars = [
                ',', '.', '<', '>', '{', '}', '[', ']', '"', "'", ':', ';', '!', '@', '#', '$', '%', '^', '&', '*', '(',
                ')', '-', '+', '=', '~'
            ]
            re_stop_words = r"\b(" + "|".join(stop_words) + r")\b\s*"
            re_token_chars = "[" + re.escape("".join(token_chars)) + "]"
            cleaned_query = re.sub(re_stop_words, "", query)
            cleaned_query = re.sub(re_token_chars, " ", cleaned_query)
            # Replace more than 1 consecutive space with just 1 space.
            cleaned_query = re.sub(" +", " ", cleaned_query)
            cleaned_query = cleaned_query.strip()
            search_terms = cleaned_query.split(" ")
            if prefix_search: cleaned_query += "*"
            if levenshtein_distance:
                # Enforced maximum LD by Redisearch.
                if levenshtein_distance > 3: levenshtein_distance = 3
                levenshtein_str = "%" * levenshtein_distance # e.g. LD = 3; "short phrase" => "%%%short%%% %%%phrase%%%"
                cleaned_query = levenshtein_str + re.sub(" ", levenshtein_str + " " + levenshtein_str, cleaned_query) + levenshtein_str

            # Have to execute multi-index searches in a rudimentary way due to the limitations of redisearch in redisgraph.
            # Divide the query limit evenly between each statement so that, for example, if a user searches two indexes for a term,
            # they won't end up with 50 results from the first index and 0 from the second because the query limit is 50.
            # Instead they'll get 25 from the first index, and 25 from the second.
            per_statement_limit = query_limit // len(indexes)
            remainder = query_limit % len(indexes)
            per_statement_limits = {index: per_statement_limit for index in indexes}
            # Distribute the remainder across each statement limit.
            # So that, for example, if the limit is 50 and there are 3 indexes, it'll be distributed as {index0: 17, index1: 17, index2: 16}
            i = 0
            while remainder > 0:
                per_statement_limits[indexes[i]] += 1
                remainder -= 1
                i += 1
                if i == len(indexes):
                    i = 0
            # Note that although the native Lucene implementation used by Neo4j will always return hits ordered by descending score
            # i.e. highest to lowest score order, RediSearch does not do this, so an ORDER BY statement is necessary.
            statements = [
                f"""
                CALL db.idx.fulltext.queryNodes('{index}', '{cleaned_query}')
                YIELD node, score
                {postprocessing_cypher}
                RETURN distinct(node), score
                ORDER BY score DESC
                LIMIT {per_statement_limits[index]}
                """
                for index in indexes
            ]
            query = "UNION".join(statements)
            logger.info(f"starting search query {query} on graph...")
            logger.debug(f"cleaned query: {cleaned_query}")
            result = self.driver.run_sync(query)
            hits = self.convert_to_dict(result)
            for hit in hits:
                hit["labels"] = dict(hit["node"])["labels"]
                hit["node"] = dict(dict(hit["node"])["properties"])
                hit["score"] = float(hit["score"])
            hits.sort(key=lambda hit: hit["score"], reverse=True)
            return {
                "hits": hits,
                "search_terms": search_terms
            }

        def get_schema(self, force_update=False):
            """
            Gets the schema of the graph. To be used by. Also generates graph summary
            :return: Dict of structure source label as outer most keys, target labels as inner keys and list of predicates
            as value.
            :rtype: dict
            """
            self.schema_raw_result = {}
            if self.schema is None or force_update:
                query = """
                           MATCH (a)-[x]->(b)                                                                                  
                           RETURN DISTINCT labels(a) as source_labels, type(x) as predicate, labels(b) as target_labels
                           """
                logger.info(f"starting query {query} on graph... this might take a few")
                result = self.driver.run_sync(query)
                logger.info(f"completed query, preparing initial schema")
                structured = self.convert_to_dict(result)
                self.schema_raw_result = structured
                schema_bag = {}
                # permituate source labels and target labels array
                # replacement for unwind for previous cypher
                structured_expanded = []
                for triplet in structured:
                    # Since there are some nodes in data currently just one label ['biolink:NamedThing']
                    # This filter is to avoid that scenario.
                    # @TODO need to remove this filter when data build avoids adding nodes with single ['biolink:NamedThing'] labels.
                    filter_named_thing = lambda x: filter(lambda y: y != 'biolink:NamedThing', x)
                    # For redis convert these to arrays
                    source_labels = [triplet['source_labels']] if isinstance(triplet['source_labels'], str) else triplet['source_labels']
                    target_labels = [triplet['target_labels']] if isinstance(triplet['target_labels'], str) else triplet['target_labels']
                    source_labels, predicate, target_labels = self.find_biolink_leaves(filter_named_thing(source_labels)), \
                                                              triplet['predicate'], \
                                                              self.find_biolink_leaves(filter_named_thing(target_labels))

                    for source_label in source_labels:
                        for target_label in target_labels:
                            structured_expanded.append(
                                {
                                    'source_label': source_label,
                                    'target_label': target_label,
                                    'predicate': predicate
                                }
                            )
                structured = structured_expanded
                for triplet in structured:
                    subject = triplet['source_label']
                    predicate = triplet['predicate']
                    objct = triplet['target_label']
                    if subject not in schema_bag:
                        schema_bag[subject] = {}
                    if objct not in schema_bag[subject]:
                        schema_bag[subject][objct] = []
                    if predicate not in schema_bag[subject][objct]:
                        schema_bag[subject][objct].append(predicate)
                self.schema = schema_bag
                logger.info("schema done.")
                if not self.summary:
                    query = """
                    MATCH (c) RETURN DISTINCT labels(c) as types, count(c) as count                
                    """
                    logger.info(f'generating graph summary: {query}')
                    raw = self.convert_to_dict(self.driver.run_sync(query))
                    summary = {}
                    for node in raw:
                        labels = node['types']
                        labels = labels if isinstance(labels, list) else[labels]
                        count = node['count']
                        query = f"""
                        MATCH (n)-[e]->(b) WITH DISTINCT e , b
                        WHERE labels(n) in {labels}
                        RETURN 
                            type(e) as edge_types, 
                            count(e) as edge_counts,
                            labels(b) as target_labels 
                        """
                        raw = self.convert_to_dict(self.driver.run_sync(query))
                        summary_key = ':'.join(labels)
                        summary[summary_key] = {
                            'nodes_count': count
                        }
                        for row in raw:
                            target_label = row['target_labels']
                            target_label = [target_label] if isinstance(target_label, str) else target_label
                            target_key = ':'.join(target_label)
                            edge_name = row['edge_types']
                            edge_count = row['edge_counts']
                            summary[summary_key][target_key] = summary[summary_key].get(target_key, {})
                            summary[summary_key][target_key][edge_name] = edge_count
                    self.summary = summary
                    logger.info(f'generated summary for {len(summary)} node types.')
            return self.schema

        async def get_mini_schema(self, source_id, target_id):
            """
            Given either id of source and/or target returns predicates that relate them. And their
            possible labels.
            :param source_id:
            :param target_id:
            :return:
            """
            source_id_syntaxed = f"{{id: \"{source_id}\"}}" if source_id else ''
            target_id_syntaxed = f"{{id: \"{target_id}\"}}" if target_id else ''
            query = f"""
                            MATCH (a{source_id_syntaxed})-[x]->(b{target_id_syntaxed}) WITH
                                [la in labels(a) where la <> 'Concept'] as source_label,
                                [lb in labels(b) where lb <> 'Concept'] as target_label,
                                type(x) as predicate
                            RETURN DISTINCT source_label, predicate, target_label
                        """
            response = await self.driver.run(query)
            response = self.convert_to_dict(response)
            return response

        async def get_node(self, node_type: str, curie: str) -> list:
            """
            Returns a node that matches curie as its ID.
            :param node_type: Type of the node.
            :type node_type:str
            :param curie: Curie.
            :type curie: str
            :return: value of the node in neo4j.
            :rtype: list
            """
            query = f"MATCH (c:`{node_type}`{{id: '{curie}'}}) return c"
            response = await self.driver.run(query)

            data = response.get('results',[{}])[0].get('data', [])
            '''
            data looks like 
            [
            {'row': [{...node data..}], 'meta': [{...}]},
            {'row': [{...node data..}], 'meta': [{...}]},
            {'row': [{...node data..}], 'meta': [{...}]}
            ]            
            '''
            rows = []
            if len(data):
                from functools import reduce
                rows = reduce(lambda x, y: x + y.get('row', []), data, [])
            return rows

        async def get_single_hops(self, source_type: str, target_type: str, curie: str) -> list:
            """
            Returns a triplets of source to target where source id is curie.
            :param source_type: Type of the source node.
            :type source_type: str
            :param target_type: Type of target node.
            :type target_type: str
            :param curie: Curie of source node.
            :type curie: str
            :return: list of triplets where each item contains source node, edge, target.
            :rtype: list
            """

            query = f'MATCH (c:`{source_type}`{{id: \'{curie}\'}})-[e]->(b:`{target_type}`) return distinct c , e, b'
            response = await self.driver.run(query)
            rows = list(map(lambda data: data['row'], response['results'][0]['data']))
            query = f'MATCH (c:`{source_type}`{{id: \'{curie}\'}})<-[e]-(b:`{target_type}`) return distinct b , e, c'
            response = await self.driver.run(query)
            rows += list(map(lambda data: data['row'], response['results'][0]['data']))

            return rows

        async def run_cypher(self, cypher: str, **kwargs) -> list:
            """
            Runs cypher directly.
            :param cypher: cypher query.
            :type cypher: str
            :return: unprocessed neo4j response.
            :rtype: list
            """
            return await self.driver.run(cypher, **kwargs)

        async def get_sample(self, node_type):
            """
            Returns a few nodes.
            :param node_type: Type of nodes.
            :type node_type: str
            :return: Node dict values.
            :rtype: dict
            """
            query = f"MATCH (c:{node_type}) return c limit 5"
            response = await self.driver.run(query)
            rows = response['results'][0]['data'][0]['row']
            return rows

        async def get_examples(self, source, target=None):
            """
            Returns an example for source node only if target is not specified, if target is specified a sample one hop
            is returned.
            :param source: Node type of the source node.
            :type source: str
            :param target: Node type of the target node.
            :type target: str
            :return: A single source node value if target is not provided. If target is provided too, a triplet.
            :rtype:
            """
            if target:
                query = f"MATCH (source:{source})-[edge]->(target:{target}) return source, edge, target limit 1"
                response = await self.run_cypher(query)
                final = list(map(lambda data: data['row'], response['results'][0]['data']))
                return final
            else:
                query = f"MATCH ({source}:{source}) return {source} limit 1"
                response = await self.run_cypher(query)
                final = list(map(lambda node: node[source], self.driver.convert_to_dict(response)))
                return final

        def supports_apoc(self):
            """
            Returns true if apoc is supported by backend database.
            :return: bool true if neo4j supports apoc.
            """
            return self.driver.check_apoc_support()

        async def run_apoc_cover(self, ids: list):
            """
            Runs apoc.algo.cover on list of ids
            :param ids:
            :return: dictionary of edges and source and target nodes ids
            """
            query = f"""
                        MATCH (node:`biolink:NamedThing`)
                        USING INDEX node:`biolink:NamedThing`(id)
                        WHERE node.id in {ids}
                        WITH collect(node) as nodes
                        CALL apoc.algo.cover(nodes) yield rel
                        WITH {{subject: startNode(rel).id ,
                               object: endNode(rel).id,
                               predicate: type(rel),
                               edge: rel }} as row
                        return collect(row) as result                                        
                        """
            result = self.convert_to_dict(self.driver.run_sync(query))
            return result

        def convert_to_dict(self, result):
            return self.driver.convert_to_dict(result)

        async def answer_trapi_question(self, trapi_question, options={}, timeout=None):
            response = await self.driver.answer_TRAPI_question(trapi_question, options=options, timeout=timeout)
            response.update({'query_graph': trapi_question})
            return response

    instance = None

    def __init__(self, host, port, auth, db_name, db_type):
        # create a new instance if not already created.
        if not GraphInterface.instance:
            GraphInterface.instance = GraphInterface._GraphInterface(host=host, port=port, auth=auth, backend= db_type ,db_name=db_name)

    def __getattr__(self, item):
        # proxy function calls to the inner object.
        return getattr(self.instance, item)
