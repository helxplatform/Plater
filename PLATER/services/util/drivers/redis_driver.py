import redis
from PLATER.services.config import config
from PLATER.services.util.logutil import LoggingUtil
from PLATER.services.util.drivers.redis_trapi_cypher_compiler import cypher_query_answer_map
from redisgraph import Graph, Node, Edge


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

    def transplile_TRAPI_cypher(self, trapi_question):
        return cypher_query_answer_map(trapi_question)

    async def answer_TRAPI_question(self, trapi_question):
        cypher = self.transplile_TRAPI_cypher(trapi_question)
        logger.info("RUNNING TRAPI QUERY: ")
        logger.info(cypher)
        results = await self.run(cypher)
        results_dict = self.convert_to_dict(results)
        return self.create_TRAPI_kg_response(trapi_question, results_dict)

    def create_TRAPI_kg_response(self, query_graph , results_dict):
        node_qg_ids = list(map(lambda x: x['id'], query_graph['nodes']))
        edge_qg_ids = list(map(lambda x: x['id'], query_graph['edges']))
        answer_bindings = []
        nodes_all = []
        edges_all = []
        collected_nodes = set()
        collected_edges = set()

        for row in results_dict:
            # {n0: {dict} , n1: [list{dict}] , e0: [list{dict}] etc...
            current_answer_bindings = {
                'node_bindings': [],
                'edge_bindings': []
            }
            bound_nodes = {}
            for qg_id in node_qg_ids:
                nodes = row[qg_id] if isinstance(row[qg_id], list) else [row[qg_id]]
                node_types = row[f'type__{qg_id}'] if isinstance(row[qg_id], list) else [row[f'type__{qg_id}']]
                for node, node_type in zip(nodes, node_types):
                    node_id = node['id']
                    current_answer_bindings['node_bindings'] += [{'qg_id': qg_id, 'kg_id': node_id, 'type': node_type}]
                    bound_nodes[qg_id] = bound_nodes.get(qg_id, [])
                    bound_nodes[qg_id].append(node_id)
                    if node_id not in collected_nodes:
                        collected_nodes.add(node_id)
                        node.update({'type': node_type})
                        nodes_all.append(node)
            for qg_id in edge_qg_ids:
                edges = row[qg_id] if isinstance(row[qg_id], list) else [row[qg_id]]
                edge_types = row[f'type__{qg_id}'] if isinstance(row[qg_id], list) else [row[f'type__{qg_id}']]
                index = 0
                for edge, edge_type in zip(edges, edge_types):
                    edge_id = edge['id']
                    current_answer_bindings['edge_bindings'] += [{'qg_id': qg_id, 'kg_id': edge_id, 'type': edge_type}]
                    if edge_id not in collected_edges:
                        edge_in_query_graph = list(filter(lambda x: x['id'] == qg_id, query_graph['edges']))[0]
                        source_q_id, target_q_id = edge_in_query_graph['source_id'], edge_in_query_graph['target_id']
                        source_real_id, target_real_id = bound_nodes[source_q_id][index if len(bound_nodes[source_q_id]) > 1 else 0]\
                            , bound_nodes[target_q_id][index if len(bound_nodes[target_q_id]) > 1 else 0]
                        edge.update({'source_id': source_real_id, 'target_id': target_real_id, 'type': edge_type})
                        collected_edges.add(edge_id)
                        edges_all.append(edge)
                        index += 1
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