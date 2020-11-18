from PLATER.services.util.drivers.redis_trapi_cypher_compiler \
    import cypher_query_answer_map,\
    cypher_prop_string, \
    cypher_query_fragment_match, \
    NodeReference, \
    EdgeReference


def test_node_reference():
    node = {
        'id': 'xxxx',
        'type': 'chemical_substance',
        'curie': 'CURIE:1',
        'set': False
    }
    cypher_node = NodeReference(node)

    node_rep = cypher_node.__str__()
    assert node_rep == f'{cypher_node.name}' + f'{cypher_node.prop_string}'
    node_rep = cypher_node.__str__()
    assert  node_rep == cypher_node.name
    assert cypher_node.filters == "'chemical_substance' in xxxx.category"
    assert cypher_node.prop_string == " {`id`: 'CURIE:1'}"
    assert cypher_node.name == node['id']
    assert cypher_node.labels == [node['type']]
    # node with multiple extra vars and types
    node = {
        'type': ['chemical_substance', 'named_thing'],
        'curie': ['CURIE:1', 'CURIE:2'],
        'id': 'n0',
        'extra_prop': 'prop1',
        'set': False
    }
    cypher_node = NodeReference(node)
    node_rep = cypher_node.__str__()
    assert node_rep == f'{cypher_node.name}' + f'{cypher_node.prop_string}'
    node_rep = cypher_node.__str__()
    assert node_rep == cypher_node.name
    split_by_and = cypher_node.filters.split(' AND ')
    # note for performance we should go for curie filter first then category (since looking up array is expensive?)
    all_filters = list(map(lambda x: x.strip(' '), split_by_and[0].strip('(').strip(')').split(' OR ')))
    assert "n0.id = 'CURIE:1'" in all_filters
    assert "n0.id = 'CURIE:2'" in all_filters
    all_filters = list(map(lambda x: x.strip(' '), split_by_and[1].strip('(').strip(')').split(' OR ')))
    assert "'chemical_substance' in n0.category" in all_filters
    assert "'named_thing' in n0.category" in all_filters
    assert cypher_node.prop_string == ' {`extra_prop`: \'prop1\'}'

def test_edge_reference():
    # typed edge
    edge = {
        'id': 'e0',
        'type': 'biolink:some_edge',
        'source_id': 'n0',
        'target_id': 'n1'
    }
    cypher_edge = EdgeReference(edge)
    # first access returns match pattern
    assert f'-[{edge["id"]}:`{edge["type"]}`]->' == str(cypher_edge)
    # second access returns with out type
    assert f'-[{edge["id"]}]->' == str(cypher_edge)
    assert cypher_edge.filters == ''
    # when edges have multiple types
    edge = {
        'id': 'e0',
        'type': ['biolink:some_edge', 'biolink:other_type'],
        'source_id': 'n0',
        'target_id': 'n1'
    }
    cypher_edge = EdgeReference(edge)
    # first and second access should stay same
    assert f'-[{edge["id"]}]->' == str(cypher_edge)
    assert f'-[{edge["id"]}]->' == str(cypher_edge)
    all_filters = list(map(lambda x: x.strip(' '), cypher_edge.filters.split(' OR ')))
    assert f'type(e0) = "{edge["type"][0]}"' in all_filters
    assert f'type(e0) = "{edge["type"][1]}"' in all_filters

def test_match_header():
    trapi_question = {
        'nodes': [
            {'id': 'n0', 'type': 'x', 'curie': 'y'},
            {'id': 'n1', 'type': 'y'},
        ], 'edges': [
            {'id': 'e0', 'source_id': 'n0', 'target_id': 'n1', 'type': ['biolink:type', 'biolink:Other']}
        ]
    }
    match_clause = cypher_query_fragment_match(trapi_question)
    cypher_nodes = list(map(lambda x: NodeReference(x), trapi_question['nodes']))
    cypher_edges = list(map(lambda x: EdgeReference(x), trapi_question['edges']))
    match_head = match_clause.split('\n')[0]
    for node in cypher_nodes:
        assert node.__str__() in match_head
    for edge in cypher_edges:
        assert edge.__str__() in match_head


def test_where_conditions():
    trapi_question = {
        'nodes': [
            {'id': 'n0', 'type': 'x', 'curie': 'y'},
            {'id': 'n1', 'type': 'y'},
        ], 'edges': [
            {'id': 'e0', 'source_id': 'n0', 'target_id': 'n1', 'type': ['biolink:type', 'biolink:Other']}
        ]
    }
    match_fragment = cypher_query_fragment_match(trapi_question)
    where_clauses = match_fragment.split('WHERE')[1]
    cypher_nodes = list(map(lambda x: NodeReference(x), trapi_question['nodes']))
    cypher_edges = list(map(lambda x: EdgeReference(x), trapi_question['edges']))
    all_filters = []
    for node in cypher_nodes:
        assert node.filters in where_clauses
        all_filters.append(f'({node.filters})')
    for edge in cypher_edges:
        assert edge.filters in where_clauses
        all_filters.append(f'({edge.filters})')
    # make sure all edge and node filters are joined with AND
    where_split_with_and = list(map(lambda x: x.strip(' '), where_clauses.split('\nAND ')))
    assert len(all_filters) == len(where_split_with_and)
    for f in all_filters:
        assert f in where_split_with_and


def test_returns():
    trapi_question = {
        'nodes': [
            {'id': 'n0', 'type': 'x', 'curie': 'y'},
            {'id': 'n1', 'type': 'y', 'set': True},
        ], 'edges': [
            {'id': 'e0', 'source_id': 'n0', 'target_id': 'n1', 'type': ['biolink:type', 'biolink:Other']}
        ]
    }
    match_fragment = cypher_query_fragment_match(trapi_question)
    whole_cypher = cypher_query_answer_map(trapi_question)
    returns = whole_cypher.replace(match_fragment +'\n', '').split('\n')
    with_clause, returns_clause = list(map(lambda x: x.strip(' '), returns[0].replace('WITH ', '').split(',')))\
        , list(map(lambda x: x.strip(' '), returns[1].replace('RETURN','').split(',')))
    # make sure normal nodes are return
    assert 'n0 AS n0' in with_clause
    # make sure sets are collected
    assert 'collect(n1) AS n1' in with_clause
    # make sure edges are collected aswell
    assert 'collect(e0) AS e0' in with_clause


    assert 'n0' in returns_clause
    assert 'n1' in returns_clause
    assert 'e0' in returns_clause

    # check for types
    assert 'labels(n0) AS type__n0' in returns_clause
    # types for collected node should be an array
    assert '[node in n1 | labels(node)] AS type__n1' in returns_clause
    # types for edges is similer
    assert '[edge in e0 | type(edge)] AS type__e0' in returns_clause


