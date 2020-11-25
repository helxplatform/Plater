# This test will test a running instance of plater with
# running cypher on the cypher endpoint and asserting results
# to equivalent TRAPI messages
#

import pytest
import os
from functools import reduce
import requests

PLATERURL = os.environ.get('PLATER_HOST', 'http://localhost:81')
if not PLATERURL:
    raise Exception("Error : Please set PLATER_HOST env var")

@pytest.fixture
def connected_nodes_ids():
    connected_node_cypher = {
        "query": "MATCH (a)--() return distinct a limit 20"
    }

    response = requests.post(PLATERURL + '/cypher', json=connected_node_cypher)
    assert response.status_code == 200, f"ERROR running cypher to endpoint {PLATERURL}/cypher"
    response = response.json()
    results = response['results'][0]
    cols = results['columns']
    rows = reduce(lambda x, y: x + y, [d['row'] for d in results['data']], [])
    ids = list(map(lambda x : x['id'], rows))
    return ids


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

def test_single_hop(connected_nodes_ids):
    for i in connected_nodes_ids:
        cypher_query = {
            "query": f"MATCH (a{{id : '{i}'}})-[e]->(b) return a, e, b limit 10 "
        }
        cypher_resp = requests.post(PLATERURL + '/cypher', json=cypher_query)
        assert cypher_resp.status_code == 200
        cypher_resp = cypher_resp.json()
        cypher_resp = convert_to_dict(cypher_resp)
        query_graph = {
            "query_graph":{
                'nodes': [
                    {'id': 'a', 'curie': f"{i}", 'type': 'named_thing'},
                    {'id': 'b', 'type': 'named_thing'}
                ],
                'edges': [
                    {'id': 'e', 'source_id': 'a', 'target_id': 'b'}
                ]
            }
        }
        import json
        print(json.dumps({"message":query_graph}, indent=2))
        trapi_response = requests.post(PLATERURL + '/query', json={'message': query_graph})
        assert trapi_response.status_code == 200
        trapi_response = trapi_response.json()
        assert len(trapi_response['knowledge_graph']['nodes']) != 0
        assert len(trapi_response['knowledge_graph']['edges']) != 0
        assert len(trapi_response['results'])
        path_array = []
        # check edges and nodes exist in knowledge graph
        for row in cypher_resp:
            found_a = False
            found_b = False
            found_e = False
            for node in trapi_response['knowledge_graph']['nodes']:
                if row['a']['id'] == node['id']:
                    found_a = True
                    for key in row['a'].keys():
                        assert row['a'][key] == node[key]
            assert found_a
            for node in trapi_response['knowledge_graph']['nodes']:
                if row['b']['id'] == node['id']:
                    found_b = True
                    for key in row['b'].keys():
                        assert row['b'][key] == node[key]
            assert found_b
            for edge in trapi_response['knowledge_graph']['edges']:
                if row['e']['id'] == edge['id']:
                    found_e = True
                    for key in row['e'].keys():
                        assert row['e'][key] == edge[key]
            assert found_e
            path_array.append([row['a']['id'], row['e']['id'], row['b']['id'] ])
            for result in trapi_response['results']:
                assert result['node_bindings']
                assert result['edge_bindings']
                binding_as_dict = {r['qg_id']: r['kg_id'] for r in result['node_bindings']}
                binding_as_dict.update({r['qg_id']: r['kg_id'] for r in result['edge_bindings']})
                current_path = [binding_as_dict['a'], binding_as_dict['e'], binding_as_dict['b']]
                assert current_path in path_array
