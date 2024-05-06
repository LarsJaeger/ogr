from __future__ import annotations
from typing import TYPE_CHECKING

import attrs

if TYPE_CHECKING:
    from ogr.node import Node
    from ogr.graph import Graph


class MetaConnection(type):
    def __new__(cls, name, bases, dct):
        x = super().__new__(cls, name, (Connection,), dct)
        x.type = name
        return x


@attrs.define(kw_only=True)
class Connection:
    uuid: str | None = attrs.field(default=None)
    node_a: Node
    node_b: Node

    def create(self, graph: Graph):
        """
        Creates the connection in the database.
        """
        graph.create_connection(self)

    def update(self, graph: Graph):
        """
        Updates the connections's properties in the database.
        """
        graph.set_connection(self)

    def delete(self, graph: Graph):
        """
        Deletes the connection from the database.
        """
        graph.delete_connection(self)
