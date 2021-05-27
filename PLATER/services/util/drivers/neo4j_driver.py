import base64
import traceback
import httpx
from PLATER.services.config import config
from PLATER.services.util.logutil import LoggingUtil
from reasoner.cypher import get_query

logger = LoggingUtil.init_logging(__name__,
                                  config.get('logging_level'),
                                  config.get('logging_format')
                                  )


class Neo4jHTTPDriver:
    def __init__(self, host: str, port: int,  auth: set, scheme: str = 'http'):
        self._host = host
        self._neo4j_transaction_endpoint = "/db/data/transaction/commit"
        self._scheme = scheme
        self._full_transaction_path = f"{self._scheme}://{self._host}:{port}{self._neo4j_transaction_endpoint}"
        self._port = port
        self._supports_apoc = None
        self._header = {
                'Accept': 'application/json; charset=UTF-8',
                'Content-Type': 'application/json',
                'Authorization': 'Basic %s' % base64.b64encode(f"{auth[0]}:{auth[1]}".encode('utf-8')).decode('utf-8')
            }
        # ping and raise error if neo4j doesn't respond.
        logger.debug('PINGING NEO4J')
        self.ping()
        logger.debug('CHECKING IF NEO4J SUPPORTS APOC')
        self.check_apoc_support()
        logger.debug(f'SUPPORTS APOC : {self._supports_apoc}')

    async def post_request_json(self, payload):
        async with httpx.AsyncClient(timeout=600) as session:
            response = await session.post(self._full_transaction_path, json=payload, headers=self._header)
            if response.status_code != 200:
                logger.error(f"[x] Problem contacting Neo4j server {self._host}:{self._port} -- {response.status_code}")
                txt = response.text
                logger.debug(f"[x] Server responded with {txt}")
            else:
                return response.json()

    def ping(self):
        """
        Pings the neo4j backend.
        :return:
        """
        neo4j_db_labels_endpoint = "/db/data/labels"
        ping_url = f"{self._scheme}://{self._host}:{self._port}{neo4j_db_labels_endpoint}"
        logger.info(ping_url)
        # if we can't contact neo4j, we should exit.
        try:
            import time
            now = time.time()
            response = httpx.get(ping_url, headers=self._header)
            later = time.time()
            time_taken = later - now
            logger.debug(f'Contacting neo4j took {time_taken} seconds.')
            if time_taken > 5:  # greater than 5 seconds it's not healthy
                logger.warn(f"Contacting neo4j took more than 5 seconds ({time_taken}). Neo4j might be stressed.")
            if response.status_code != 200:
                raise Exception(f'server returned {response.status_code}')
        except Exception as e:
            logger.error(f"Error contacting Neo4j @ {ping_url} -- Exception raised -- {e}")
            logger.debug(traceback.print_exc())
            raise RuntimeError('Connection to Neo4j could not be established.')

    async def run(self, query, return_errors=False):
        """
        Runs a neo4j query async.
        :param query: Cypher query.
        :type query: str
        :return: result of query.
        :rtype: dict
        """
        # make the statement dictionary
        payload = {
            "statements": [
                {
                    "statement": f"{query}"
                }
            ]
        }

        response = await self.post_request_json(payload)
        errors = response.get('errors')
        if errors:
            if return_errors:
                return response
            logger.error(f'Neo4j returned `{errors}` for cypher {query}.')
            raise RuntimeWarning(f'Error running cypher {query}.')
        return response

    def run_sync(self, query):
        """
        Runs a neo4j query. Can cause the async loop to block.
        :param query:
        :return:
        """
        payload = {
            "statements": [
                {
                    "statement": f"{query}"
                }
            ]
        }
        response = httpx.post(
            self._full_transaction_path,
            headers=self._header,
            timeout=600,
            json=payload).json()
        errors = response.get('errors')
        if errors:
            logger.error(f'Neo4j returned `{errors}` for cypher {query}.')
            raise RuntimeWarning(f'Error running cypher {query}.')
        return response

    def convert_to_dict(self, response: dict) -> list:
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

    def check_apoc_support(self):
        apoc_version_query = 'call apoc.help("meta")'
        if self._supports_apoc is None:
            try:
                self.run_sync(apoc_version_query)
                self._supports_apoc = True
            except:
                self._supports_apoc = False
        return self._supports_apoc

    async def answer_TRAPI_question(self, trapi_question, options={}):
        cypher_query = get_query(trapi_question)
        logger.info("RUNNING TRAPI QUERY:")
        logger.info(cypher_query)
        results = await self.run(cypher_query)
        results_dict = self.convert_to_dict(results)[0]
        results_dict.update({'query_graph': trapi_question})
        return results_dict


