"""Tools for compiling QGraph into Cypher query."""
import re
from reasoner_converter.downgrading import downgrade_BiolinkEntity, downgrade_BiolinkPredicate
from reasoner_converter.upgrading import upgrade_BiolinkEntity, upgrade_BiolinkRelation


def cypher_prop_string(value):
    """Convert property value to cypher string representation."""
    if isinstance(value, bool):
        return str(value).lower()
    elif isinstance(value, str):
        return f"'{value}'"
    else:
        raise ValueError(f'Unsupported property type: {type(value).__name__}.')


class NodeReference():
    """Node reference object."""

    def __init__(self, node, node_id ,anonymous=False):
        """Create a node reference."""
        node = dict(node)
        name = f'{node_id}' if not anonymous else ''
        labels = node.pop('category', 'named_thing')
        if not isinstance(labels, list):
            labels = [labels]
        props = {}
        curie_filters = []
        curie = node.pop("id", None)
        if curie is not None:
            if isinstance(curie, str):
                props['id'] = curie
            elif isinstance(curie, list):
                for ci in curie:
                    # generate curie-matching condition
                    curie_filters.append(f"{name}.id = '{ci}'")
                # union curie-matching filters together
            else:
                raise TypeError("Curie should be a string or list of strings.")
        label_filters = []
        if labels:
            biolink_regex = "^biolink:[A-Z][a-zA-Z]*$"
            for label in labels:
                label_filters.append(f"'{label}' in {name}.category")
                is_biolinkified = re.match(biolink_regex, label)
                if is_biolinkified:
                    other_label =  downgrade_BiolinkEntity(label)
                else:
                    other_label = upgrade_BiolinkEntity(label)
                label_filters.append(f"'{other_label}' in {name}.category")

        if len(curie_filters):
            filters = '( ' + ' OR '.join(curie_filters) + ') AND (' + ' OR '.join(label_filters) + ')'
        else:
            filters = ' OR '.join(label_filters)
        self._filters = filters
        node.pop('name', None)
        node.pop('is_set', False)
        props.update(node)

        self.name = name
        self.labels = labels
        self.prop_string = ' {' + ', '.join([f"`{key}`: {cypher_prop_string(props[key])}" for key in props]) + '}'
        self._filters = filters
        if curie:
            self._extras = '' #f' USING INDEX {name}:{labels[0]}(id)'
        else:
            self._extras = ''
        self._num = 0

    def __str__(self):
        """Return the cypher node reference."""
        self._num += 1
        if self._num == 1:
            return f'{self.name}' + f'{self.prop_string}' # + ''.join(f':`{label}`' for label in self.labels)
        return self.name

    @property
    def filters(self):
        """Return filters for the cypher node reference.
        To be used in a WHERE clause following the MATCH clause.
        """
        return self._filters

    @property
    def extras(self):
        """Return extras for the cypher node reference.
        To be appended to the MATCH clause.
        """
        if self._num >= 1:
            return self._extras
        else:
            return ''


class EdgeReference:
    """Edge reference object."""

    def __init__(self, edge, edge_id, anonymous=False):
        """Create an edge reference."""

        name = f'{edge_id}' if not anonymous else ''
        label = edge['predicate'] if 'predicate' in edge else None

        if 'predicate' in edge and edge['predicate'] is not None:
            if isinstance(edge['predicate'], str):
                label = edge['predicate']
                filters = ''
            elif isinstance(edge['predicate'], list):
                filters = []
                biolink_predicate_regex = "^biolink:[a-z][a-z_]*$"
                for predicate in edge['predicate']:
                    is_biolink_predicate = re.match(biolink_predicate_regex, predicate)
                    filters.append(f'type({name}) = "{predicate}"')
                    if is_biolink_predicate:
                        other_predicate = downgrade_BiolinkPredicate(predicate)
                    else:
                        other_predicate = upgrade_BiolinkRelation(predicate)
                    filters.append(f'type({name}) = "{other_predicate}" ')
                filters = ' OR '.join(filters)
                label = None
        else:
            label = None
            filters = ''

        self.name = name
        self.label = label
        self._num = 0
        self._filters = filters
        has_type = 'predicate' in edge and edge['predicate']
        self.directed = edge.get('directed', has_type)

    def __str__(self):
        """Return the cypher edge reference."""
        self._num += 1
        if self._num == 1:
            label = f':`{self.label}`' if self.label else ''
            innards = f'{self.name}{label}'
        else:
            innards = self.name
        if self.directed:
            return f'-[{innards}]->'
        else:
            return f'-[{innards}]-'

    @property
    def filters(self):
        """Return filters for the cypher node reference.
        To be used in a WHERE clause following the MATCH clause.
        """
        return self._filters


def cypher_query_fragment_match(qgraph, max_connectivity=-1):
    """Generate a Cypher query fragment to match the nodes and edges that correspond to a question.
    This is used internally for cypher_query_answer_map and cypher_query_knowledge_graph
    Returns the query fragment as a string.
    """
    nodes, edges = qgraph['nodes'], qgraph['edges']

    # generate internal node and edge variable names
    node_references = {n: NodeReference(node=nodes[n], node_id=n) for n in nodes}
    edge_references = {e: EdgeReference(edge=edges[e], edge_id=e) for e in edges}

    match_strings = []

    # match orphaned nodes
    def flatten(l):
        return [e for sl in l for e in sl]
    all_nodes = set(nodes.keys())
    all_referenced_nodes = set(flatten([[edges[e]['subject'], edges[e]['object']] for e in edges]))
    orphaned_nodes = all_nodes - all_referenced_nodes
    nodes_with_id = [n_id for n_id, node in nodes.items() if node.get('id')]
    for n in orphaned_nodes:
        match_strings.append(f"MATCH ({node_references[n]})")
        match_strings[-1] += node_references[n].extras
        if node_references[n].filters:
            match_strings.append("WHERE " + node_references[n].filters)
    for n in nodes_with_id:
        match_strings.append(f"MATCH ({node_references[n]})")
        match_strings.append("WHERE " + node_references[n].filters)
        match_strings.append(f"WITH {n}")

    # match edges
    for edge_id, eref in edge_references.items():
        e = edges[edge_id]
        source_node = node_references[e['subject']]
        target_node = node_references[e['object']]
        match_strings.append(f"MATCH ({source_node}){eref}({target_node})")
        match_strings[-1] += source_node.extras + target_node.extras
        filters = [f'({c})' for c in [source_node.filters, target_node.filters, eref.filters] if c]
        if max_connectivity > -1:
            filters.append(f"(size( ({target_node})-[]-() ) < {max_connectivity})")
        if filters:
            match_strings.append("\nWHERE " + "\nAND ".join(filters))

    match_string = ' '.join(match_strings)

    return match_string


def cypher_query_answer_map(qgraph, **kwargs):
    """Generate a Cypher query to extract the answer maps for a question.
    Returns the query as a string.
    """
    clauses = []

    match_string = cypher_query_fragment_match(qgraph, max_connectivity=kwargs.pop('max_connectivity', -1))
    if match_string:
        clauses.append(match_string)

    nodes, edges = qgraph['nodes'], qgraph['edges']

    # generate internal node and edge variable names
    node_names = set(nodes.keys())
    node_names_sets = set(f"{n}" for n in filter(lambda n: nodes[n].get('is_set'), nodes))
    edge_names = set(edges.keys())

    # deal with sets
    node_id_accessor = [f"collect({n}) AS {n}" if n in node_names_sets
                        else f"{n} AS {n}" for n in node_names]
    edge_id_accessor = [f"collect({e}) AS {e}" for e in edge_names]
    if node_id_accessor or edge_id_accessor:
        with_string = f"WITH {', '.join(node_id_accessor+edge_id_accessor)}"
        clauses.append(with_string)

    returns = list(node_names) + \
              list(edge_names) + \
              [f'[node in {x} | labels(node)] AS type__{x}' for x in node_names_sets] + \
              [f'labels({x}) AS type__{x}' for x in node_names - node_names_sets] + \
              [f'[edge in {x} | type(edge)] AS type__{x}' for x in edge_names] + \
              [f'[edge in {x} | [startNode(edge).id, endNode(edge).id]] AS id_pairs__{x}' for x in edge_names]

    answer_return_string = f"RETURN " + ','.join(returns)

    clauses.append(answer_return_string)

    # return answer maps matching query
    query_string = '\n'.join(clauses)
    if 'skip' in kwargs:
        query_string += f' SKIP {kwargs["skip"]}'
    if 'limit' in kwargs:
        query_string += f' LIMIT {kwargs["limit"]}'
    return query_string