import base64
import traceback

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

        def get_schema(self):
            """
            Gets the schema of the graph. To be used by. Also generates graph summary
            :return: Dict of structure source label as outer most keys, target labels as inner keys and list of predicates
            as value.
            :rtype: dict
            """
            self.schema_raw_result = {}
            if self.schema is None:
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
                    # do reverse
                    if objct not in schema_bag:
                        schema_bag[objct] = {}
                    if subject not in schema_bag[objct]:
                        schema_bag[objct][subject] = []
                    if predicate not in schema_bag[objct][subject]:
                        schema_bag[objct][subject].append(predicate)
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
                        MATCH (:{':'.join(labels)})-[e]->(b) WITH DISTINCT e , b 
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

        async def answer_trapi_question(self, trapi_question, options={}):
            response = await self.driver.answer_TRAPI_question(trapi_question, options=options)
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
