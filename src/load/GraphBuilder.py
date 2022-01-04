from neo4j import GraphDatabase, Driver


class GraphBuilder:
    """
    @author zuyzheng
    """

    _driver: Driver

    def __init__(self, uri: str, user: str, password: str):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def build_coin_transfers(self):
        """

        """