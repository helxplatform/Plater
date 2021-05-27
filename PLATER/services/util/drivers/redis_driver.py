import redis
from PLATER.services.config import config
from PLATER.services.util.logutil import LoggingUtil
from PLATER.services.util.drivers.redis_trapi_cypher_compiler import cypher_query_answer_map
from redisgraph import Graph, Node, Edge
from reasoner_converter.upgrading import upgrade_BiolinkRelation, upgrade_BiolinkEntity

logger = LoggingUtil.init_logging(__name__,
                                  config.get('logging_level'),
                                  config.get('logging_format')
                                  )


class RedisDriver:
    def __init__(self, host, port=6379, password=None, graph_db_name='test'):
        self.redis_url = f'redis://:{password}@{host}:{port}' if password else f'redis://{host}:{port}'
        self.redis_client = None
        self.sync_redis_client = redis.Redis(host=host,
                                             port=port,
                                             password=password,
                                             encoding='utf-8',
                                             decode_responses=True)
        self.graph_name = graph_db_name
        self.redis_graph = Graph(self.graph_name, self.sync_redis_client)
        self.ping_redis()

    def ping_redis(self):
        logger.info('[x] Pinging redis')
        response = self.sync_redis_client.execute_command('ping')
        logger.info(f'[x] Got response...{response}')

    @staticmethod
    def format_cypher_result(redis_results):
        return {
            'results': [{
                'columns': redis_results[0],
                'data': [{'row': x, 'meta': []} for x in redis_results[1]]
            }],
            'errors': []
        }

    @staticmethod
    def decode_if_byte(value):
        try:
            return value.decode('utf-8')
        except:
            return value

    async def run(self, query, **kwargs):
        results = self.redis_graph.query(query, read_only=True)
        headers = list(map(lambda x: RedisDriver.decode_if_byte(x[1]), results.header))
        response = []
        for row in results.result_set:
            new_row = []
            for value in row:
                if isinstance(value, list):
                    parsed_value = []
                    for v in value:
                        if isinstance(v, Node) or isinstance(v, Edge):
                            parsed_value.append(v.properties)
                        else:
                            parsed_value.append(v)
                    new_row.append(parsed_value)
                elif isinstance(value, Node) or isinstance(value, Edge):
                    new_row.append(value.properties)
                else:
                    new_row.append(value)
            response.append(new_row)
        return self.format_cypher_result((headers, response))

    def run_sync(self, cypher_query):
        results = self.sync_redis_client.execute_command('GRAPH.RO_QUERY', self.graph_name, cypher_query)
        return RedisDriver.format_cypher_result(results)

    @staticmethod
    def convert_to_dict(response: dict) -> list:
        """
        Converts a neo4j result to a structured result.
        :param response: neo4j http raw result.
        :type response: dict
        :return: reformatted dict
        :rtype: dict
        """
        results = response.get('results')
        array = []
        if results:
            for result in results:
                cols = result.get('columns')
                if cols:
                    data_items = result.get('data')
                    for item in data_items:
                        new_row = {}
                        row = item.get('row')
                        for col_name, col_value in zip(cols, row):
                            new_row[col_name] = col_value
                        array.append(new_row)
        return array

    def transplile_TRAPI_cypher(self, trapi_question, options={}):
        return cypher_query_answer_map(trapi_question, **options)

    async def answer_TRAPI_question(self, trapi_question, options={}):
        cypher = self.transplile_TRAPI_cypher(trapi_question, options)
        logger.info("RUNNING TRAPI QUERY: ")
        logger.info(cypher)
        results = await self.run(cypher)
        results_dict = self.convert_to_dict(results)
        response = self.create_TRAPI_kg_response(trapi_question, results_dict)
        return response

    def create_TRAPI_kg_response(self, query_graph , results_dict):
        node_qg_ids = list(query_graph['nodes'].keys())
        edge_qg_ids = list(query_graph['edges'].keys())
        answer_bindings = []
        nodes_all = {}
        edges_all = {}
        collected_nodes = set()
        collected_edges = set()

        for row in results_dict:
            current_answer_bindings = {
                'node_bindings': {},
                'edge_bindings': {}
            }
            bound_nodes = {}
            for qg_id in node_qg_ids:
                # Convert nodes and node types to list
                nodes = row[qg_id] if isinstance(row[qg_id], list) else [row[qg_id]]
                node_types = row[f'type__{qg_id}'] if isinstance(row[qg_id], list) else [row[f'type__{qg_id}']]
                current_node_binding = {qg_id: []}
                for node, node_type in zip(nodes, node_types):
                    node_id = node.pop('id')
                    assert node_id, 'Error, did not find ID from Node in db'
                    current_node_binding[qg_id].append({'id': node_id})
                    bound_nodes[qg_id] = bound_nodes.get(qg_id, [])
                    bound_nodes[qg_id].append(node_id)
                    if node_id not in collected_nodes:
                        new_node = {}
                        collected_nodes.add(node_id)
                        new_node['category'] = [upgrade_BiolinkEntity(x) for x in node['category']]
                        new_node['name'] = node.get('name', '')
                        new_node['attributes'] = []
                        for key, value in node.items():
                            if key in new_node:
                                continue
                            new_node['attributes'].append({
                                'type': 'NA',
                                'value': value,
                                'name': key
                            })
                        nodes_all[node_id] = new_node
                current_answer_bindings['node_bindings'].update(current_node_binding)
            for qg_id in edge_qg_ids:
                edges = row[qg_id] if isinstance(row[qg_id], list) else [row[qg_id]]
                edge_types = row[f'type__{qg_id}'] if isinstance(row[qg_id], list) else [row[f'type__{qg_id}']]
                id_pairs = row[f'id_pairs__{qg_id}']
                index = 0
                current_edge_binding = {qg_id: []}
                for edge, edge_type, id_pair in zip(edges, edge_types, id_pairs):
                    edge_id = edge.pop('id')
                    current_edge_binding[qg_id].append({'id': edge_id})
                    if edge_id not in collected_edges:
                        edge_in_query_graph = query_graph['edges'][qg_id]
                        source_real_id, target_real_id = id_pair
                        edge_type = upgrade_BiolinkRelation(edge_type)
                        new_edge = {
                            'subject': source_real_id,
                            'object': target_real_id,
                            'predicate': edge_type,
                            'attributes': []
                        }
                        for key, value in edge.items():
                            if key in new_edge:
                                continue
                            new_edge['attributes'].append({
                                'type': 'NA',
                                'name': key,
                                'value': value
                            })
                        collected_edges.add(edge_id)
                        edges_all[edge_id] = new_edge
                        index += 1

                current_answer_bindings['edge_bindings'].update(current_edge_binding)
            answer_bindings += [current_answer_bindings]
        return {"knowledge_graph": {"nodes": nodes_all, "edges": edges_all}, "results": answer_bindings}


if __name__=='__main__':
    q= 'match (a) return count (a); '
    redis_driver = RedisDriver(host='localhost', port='6380', graph_db_name='test')
    import asyncio
    results = asyncio.run(redis_driver.run("""   
    MATCH (n0:`chemical_substance` {`id`: 'CHEBI:39385'})-[e0]-(n1:`named_thing` {}) WITH n0 AS n0, n1 AS n1, collect(e0) AS e0 RETURN n0,n1,e0
    """))
    results