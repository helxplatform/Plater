import copy
from PLATER.services.util.graph_adapter import GraphInterface
import time


class Question:

    #SPEC VARS
    QUERY_GRAPH_KEY='query_graph'
    KG_ID_KEY='kg_id'
    QG_ID_KEY='qg_id'
    ANSWERS_KEY='results'
    KNOWLEDGE_GRAPH_KEY='knowledge_graph'
    NODES_LIST_KEY='nodes'
    EDGES_LIST_KEY='edges'
    TYPE_KEY='type'
    SOURCE_KEY='source_id'
    TARGET_KEY='target_id'
    NODE_BINDINGS_KEY='node_bindings'
    EDGE_BINDINGS_KEY='edge_bindings'
    CURIE_KEY = 'curie'

    def __init__(self, question_json):
        self._question_json = copy.deepcopy(question_json)

    def compile_cypher(self):
        return get_query(self._question_json[Question.QUERY_GRAPH_KEY])

    async def answer(self, graph_interface: GraphInterface):
        """
        Updates the query graph with answers from the neo4j backend
        :param graph_interface: interface for neo4j
        :return: None
        """
        s = time.time()
        await graph_interface.answer_trapi_question(self._question_json['query_graph'])
        end = time.time()
        print(f'grabbing results took {end - s}')
        return self._question_json

    @staticmethod
    def transform_schema_to_question_template(graph_schema):
        """
        Returns array of Templates given a graph schema
        Eg: if schema looks like
           {
            "Type 1" : {
                "Type 2": [
                    "edge 1"
                ]
            }
           }
           We would get
           {
            "question_graph": {
                "nodes" : [
                    {
                        "qg_id": "n1",
                        "type": "Type 1",
                        "kg_id": "{{curie}}"
                    },
                    {
                        "qg_id" : "n2",
                        "type": "Type 2",
                        "kg_id": "{{curie}}"
                    }
                ],
                "edges":[
                    {
                        "qg_id": "e1",
                        "type": "edge 1",
                        "source_id": "n1",
                        "target_id": "n2"
                    }
                ]
            }
           }
        :param graph_schema:
        :return:
        """
        question_templates = []
        for source_type in graph_schema:
            target_set = graph_schema[source_type]
            for target_type in target_set:
                question_graph = {
                    Question.NODES_LIST_KEY: [
                        {
                            'id': "n1",
                            Question.TYPE_KEY: source_type,
                        },
                        {
                            'id': "n2",
                            Question.TYPE_KEY: target_type,
                        }
                    ],
                    Question.EDGES_LIST_KEY: []
                }
                edge_set = target_set[target_type]
                for index, edge_type in enumerate(set(edge_set)):
                    edge_dict = {
                        'id': f"e{index}",
                        Question.SOURCE_KEY: "n1",
                        Question.TARGET_KEY: "n2",
                        Question.TYPE_KEY: edge_type
                    }
                    question_graph[Question.EDGES_LIST_KEY].append(edge_dict)
            question_templates.append({Question.QUERY_GRAPH_KEY: question_graph})
        return question_templates

