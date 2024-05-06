from __future__ import annotations
from ogr.node import Node
from ogr.connection import Connection
from typing import List, Dict, Optional
from functools import partial
from uuid_extensions import uuid7str
import neo4j.graph as n4_graph
import neo4j as n4
import attrs


class Graph:
    """
    This is an in memory graph.
    """

    # TODO think about optimizing change storage (performed_ops)
    def __init__(self, model, raw_graph: Optional[n4_graph.Graph] = None):
        self.model = model
        self.nodes: Dict[str, Node] = {}  # node.uuid: node
        self.connections: Dict[str, Connection] = {}  # connection.uuid: connection
        self.performed_ops: List[callable] = []
        if raw_graph is not None:
            self.nodes = map(model.resolve_node, raw_graph.nodes)
            self.connections = map(model.resolve_connection, raw_graph.relationships)

    def create_node(self, node: Node):
        node.uuid = uuid7str()
        self.nodes[node.uuid] = node
        self.performed_ops.append(partial(_write_create_node, node=node))

    def delete_node(self, node: Node):
        del self.nodes[node.uuid]
        self.performed_ops.append(partial(_write_delete_node, node=node))

    def set_node(self, node: Node):
        self.nodes[node.uuid] = node
        self.performed_ops.append(partial(_write_set_node, node=node))

    def create_connection(self, connection: Connection):
        connection.uuid = uuid7str()
        self.connections[connection.uuid] = connection
        self.performed_ops.append(
            partial(_write_create_connection, connection=connection)
        )
        return connection

    def delete_connection(self, connection: Connection):
        del self.connections[connection.uuid]
        self.performed_ops.append(
            partial(_write_delete_connection, connection=connection)
        )

    def set_connection(self, connection: Connection):
        self.connections[connection.id] = connection
        self.performed_ops.append(partial(_write_set_connection, connection=connection))


def _write_create_connection(tx: n4.Transaction, connection: Connection) -> Connection:
    sub_dict = attrs.asdict(
        connection, filter=attrs.filters.exclude(*attrs.fields(Connection))
    )
    prop_arg_name_string = ", ".join([f"{key}: ${key}" for key in sub_dict.keys()])
    tx.run(
        f"""
        MATCH (a {{uuid: $uuid_a}}), (b {{uuid: $uuid_b}})
        CREATE (a)-[c:{connection.type} {{uuid: $uuid_c, {prop_arg_name_string}}}]->(b)
        """,
        uuid_a=connection.node_a.uuid,
        uuid_b=connection.node_b.uuid,
        uuid_c=connection.uuid,
        **sub_dict,
    ).consume()


def _write_set_connection(tx: n4.Transaction, connection: Connection):
    sub_dict = attrs.asdict(
        connection, filter=attrs.filters.exclude(*attrs.fields(Connection))
    )
    prop_arg_name_string = ", ".join([f"{key}: ${key}" for key in sub_dict.keys()])
    tx.run(
        f"""
        MATCH (a)-[c:{connection.type} {{uuid: $uuid_c}}]->(b)
        SET c = {{{prop_arg_name_string}}}
        """,
        uuid_c=connection.uuid,
        **sub_dict,
    ).consume()


def _write_delete_connection(tx: n4.Transaction, connection: Connection):
    tx.run(
        f"""
        MATCH (a)-[c:{connection.type} {{uuid: $uuid_c}}]->(b)
        DELETE c
        """,
        uuid_c=connection.uuid,
    ).consume()


def _write_create_node(tx: n4.Transaction, node: Node):
    sub_dict = attrs.asdict(node, filter=attrs.filters.exclude(*attrs.fields(Node)))
    prop_arg_name_string = ", ".join([f"{key}: ${key}" for key in sub_dict.keys()])
    tx.run(
        f"""
        CREATE (n:{":".join(type(node).labels.union(node.dyn_labels))} {{uuid: $uuid, {prop_arg_name_string}}})
        RETURN n.uuid AS uuid
        """,
        uuid=node.uuid,
        **sub_dict,
    ).consume()


def _write_set_node(tx: n4.Transaction, node: Node):
    sub_dict = attrs.asdict(node, filter=attrs.filters.exclude(*attrs.fields(Node)))
    prop_arg_name_string = ", ".join([f"n.{key} = ${key}" for key in sub_dict.keys()])
    tx.run(
        f"""
        MATCH (n:{", ".join(type(node).labels.union(node.dyn_labels))} {{uuid: $uuid}})
        FOREACH (label IN labels(n) | REMOVE n:label)
        SET n:{":".join(node.dyn_labels + node.labels)} {{{prop_arg_name_string}}}
        """,
        uuid=node.uuid,
        **sub_dict,
    ).consume()


def _write_delete_node(tx: n4.Transaction, node: Node):
    tx.run(
        f"""
        MATCH (n:{", ".join(type(node).labels.union(node.dyn_labels))} {{uuid: $uuid}})
        DETACH DELETE n
        """,
        uuid=node.uuid,
    ).consume()
