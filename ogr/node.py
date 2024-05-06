from __future__ import annotations
import attrs
from typing import Set, List, TYPE_CHECKING
from ogr.connection import Connection

if TYPE_CHECKING:
    from ogr.connection import MetaConnection


class MetaNode(type):
    def __new__(cls, name, bases, dct):
        x = super().__new__(cls, name, (Node,), dct)
        x.labels = frozenset({name})
        return x


@attrs.define(kw_only=True)
class Node:
    uuid: str | None = attrs.field(default=None)
    dyn_labels: Set[str] = attrs.field(factory=set)

    def add_labels(self, labels: Set[str]):
        """
        Add dynamic (object-specific) labels to a node.
        The node's class label is handled seperately.
        """
        self.dyn_labels = self.dyn_labels.union(labels)

    def add_label(self, label: str):
        """
        Add a dynamic (object-specific) label to a node.
        The node's class label is handled seperately.
        """
        self.dyn_labels.add(label)

    def remove_labels(self, labels: List[str]):
        """
        Remove dynamic (object-specific) labels from a node.
        The node's class label is handled seperately.
        """
        self.dyn_labels.difference_update(labels)

    def remove_label(self, label: str):
        """
        Remove a dynamic (object-specific) label from a node.
        The node's class label is handled seperately.
        """
        self.dyn_labels.remove(label)

    def create(self, graph: Graph):
        """
        Creates the node in the database.
        """
        graph.create_node(self)

    def update(self, graph: Graph):
        """
        Updates the node's labels and properties in the database.
        """
        graph.set_node(self)

    def delete(self, graph: Graph):
        """
        Deletes the node from the database.
        """
        graph.delete_node(self)

    def connect_to(self, node_end: Node, connection_type: MetaConnection = Connection):
        """
        Connects this node with `node_end`.
        Args
        :node_end: `Node`, that the connection should point to
        :connection_type: `MetaConnection`, default is the plain `Connection` class
        """
        connection_type(node_a=self, node_b=node_end)

    def connect_from(
        self, node_start: Node, connection_type: MetaConnection = Connection
    ):
        """
        Connects this node with `node_start`.
        Args
        :node_start: `Node`, that the connection should come from
        :connection_type: `MetaConnection`, default is the plain `Connection` class
        """
        connection_type(node_a=node_start, node_b=self)
