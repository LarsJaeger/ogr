from __future__ import annotations
import neo4j as n4

from ogr.graph import Graph


class Result:
    def __init__(self, n4_result: n4.Result):
        self.n4_result = n4_result

    def consume(self):
        return self.n4_result.consume()

    def fetch(self, n: int):
        """
        get and return the next n items from the result stream
        """
        raise NotImplementedError()
        # TODO
        return self.n4_result.fetch(n)

    def graph(self) -> Graph:
        return Graph(self.n4_result.graph)
