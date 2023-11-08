import asyncio
from functools import reduce
import re
import httpx
from PLATER.services.config import config


class BLHelper:
    def __init__(self, bl_url=config.get('bl_url')):
        self.bl_url = bl_url

    @staticmethod
    async def make_request(url):
        async with httpx.AsyncClient() as session:
            response = await session.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                return None

    async def get_most_specific_concept(self, concept_list: list) -> list:
        """
        Given a list of concepts find the most specific set of concepts.
        """
        tasks = []
        for concept in concept_list:
            parent_url = f"{self.bl_url}/bl/{concept}/ancestors"
            tasks.append(BLHelper.make_request(parent_url))
        response = await asyncio.gather(*tasks, return_exceptions=False)
        parents = list(reduce(lambda acc, value: acc + value, filter(lambda x: x, response), []))
        return list(filter(lambda x: x not in parents, concept_list))
    
    @staticmethod
    def upgrade_BiolinkEntity(entity):
        if entity.startswith("biolink."):
            return entity
        return "biolink." + BLHelper._pascal_case(entity)
    
    @staticmethod
    def upgrade_BiolinkRelation(biolink_relation):
        if biolink_relation is None:
            return None
        if biolink_relation.startswith("biolink."):
            return biolink_relation
        return "biolink." + BLHelper._snake_case(biolink_relation)

    
    @staticmethod
    def _pascal_case(arg: str):
        """Convert string to PascalCase.

        Non-alphanumeric characters are replaced with _.
        "ThisCase" is replaced with "this_case".
        """
        # replace _x with X
        tmp = re.sub(
            r"(?<=[a-zA-Z])_([a-z])",
            lambda c: c.group(1).upper(),
            arg
        )
        # upper-case first character
        tmp = re.sub(
            r"^[a-z]",
            lambda c: c.group(0).upper(),
            tmp
        )
        return tmp
    

    def _snake_case(arg: str):
        """Convert string to snake_case.

        Non-alphanumeric characters are replaced with _.
        CamelCase is replaced with snake_case.
        """
        # replace non-alphanumeric characters with _
        tmp = re.sub(r'\W', '_', arg)
        # replace X with _x
        tmp = re.sub(
            r'(?<=[a-z])[A-Z](?=[a-z])',
            lambda c: '_' + c.group(0).lower(),
            tmp
        )
        # lower-case first character
        tmp = re.sub(
            r'^[A-Z](?=[a-z])',
            lambda c: c.group(0).lower(),
            tmp
        )
        return tmp
