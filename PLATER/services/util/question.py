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
        answer = await graph_interface.answer_trapi_question(self._question_json['query_graph'])
        end = time.time()
        print(f'grabbing results took {end - s}')
        return answer

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


if __name__ == '__main__':
    q_graph = {
    "query_graph": {
      "nodes": [
        {
          "id": "n0",
          "type": ["gene", "named_thing"],
          "curie": ["NCBIGene:10218", "NCBIGene:2"]
        },
# "curie":"NCBIGene:2936"
         { "id" :"n1", "type":"named_thing", "set": False}
      ],
      "edges": [
          {"id": "e0", "source_id": "n1", "target_id": "n0", "type": "directly_interacts_with"}
                ]
    }
  }
    driver = GraphInterface(host='localhost',port=6379, auth=(None,None), db_name='test', db_type='redis')
    question = Question(q_graph)
    import asyncio
    import json
    json.dumps(asyncio.run(question.answer(graph_interface=driver)), indent=2)
    # schema  = {
    #   "gene": {
    #     "biological_process_or_activity": [
    #       "actively_involved_in"
    #     ],
    #     "named_thing": [
    #       "similar_to"
    #     ]
    #   },
    #   "named_thing": {
    #     "chemical_substance": [
    #       "similar_to"
    #     ],
    #     "named_thing": [
    #       "similar_to"
    #     ]
    #   }
    # }
    # import json
    # questions = Question.transform_schema_to_question_template(schema)
    # print(questions)
    # question = Question(questions[0])
    # # questions[0]['query_graph']['nodes'][1]['curie'] = ''
    # questions[0]['query_graph']['nodes'][1]['type'] = 'disease'
    # del questions[0]['query_graph']['edges'][0]['type']
    # questions[0]['query_graph']['nodes'][0]['type'] = 'information_content_entity'
    # q2 = Question(questions[0])
    # ans = q2.answer(graph_interface=GraphInterface('localhost','7474', ('neo4j', 'neo4jkp')))
    # import asyncio
    # event_loop = asyncio.get_event_loop()
    # result = event_loop.run_until_complete(ans)
    # print(json.dumps(result, indent=2))