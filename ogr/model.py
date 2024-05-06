import attrs
import neo4j as n4
import neo4j.graph as n4_graph
from functools import partial
from typing import Dict, FrozenSet, Set
from ogr.graph import Graph
from ogr.node import Node, MetaNode
from ogr.result import Result
from ogr.connection import Connection, MetaConnection
from ogr.external.metaclass import metaclass


class DataModel:
    # TODO index uuid in db
    # TODO think about how to bind database access to data model; potentially define a function to set the db
    def __init__(self, uri: str, auth: str, data_base: str = "neo4j"):
        self.uri, self.auth = uri, auth
        self.data_base_name: str = data_base
        self.registered_nodes = []
        self.label_nodes_lut: Dict[FrozenSet, Node] = {}
        self.registered_connections = []
        self.type_connections_lut: Dict[str, Connection] = {}

    def register_node(self, cls_or_labels: type | FrozenSet[str] = None):
        def wrap_class(cls: type, labels):
            cls_meta_wrapped = metaclass(MetaNode)(cls)
            cls_defined = attrs.define(kw_only=True)(cls_meta_wrapped)
            if labels:
                cls_defined.labels = labels
            self.registered_nodes.append(cls_defined)
            self.label_nodes_lut[cls_defined.labels] = cls_defined
            return cls_defined

        if isinstance(cls_or_labels, FrozenSet):
            return partial(wrap_class, labels=cls_or_labels)
        elif isinstance(cls_or_labels, type):
            return wrap_class(cls_or_labels, None)
        else:
            raise TypeError()

    def register_connection(self, cls):
        cls_meta_wrapped = metaclass(MetaConnection)(cls)
        cls_defined = attrs.define(kw_only=True)(cls_meta_wrapped)
        self.registered_connections.append(cls_defined)
        self.type_connections_lut[cls_defined.type] = cls_defined
        return cls_defined

    def write_graph(self, graph: Graph):
        """
        write changes performed on the `graph` to the database
        """
        with n4.GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()
            with driver.session(database=self.data_base_name) as session:
                with session.begin_transaction() as tx:
                    for x in graph.performed_ops:
                        x(tx)
                        print(f"wrote {x}")

    def get_node_by_uuid(self, uuid: str):
        """
        queries database for the node by uuid
        """
        with n4.GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()
            with driver.session(database=self.data_base_name) as session:
                with session.begin_transaction() as tx:
                    results: n4.Result = tx.run(
                        """
                        MATCH (a {uuid: $uuid})
                        RETURN a
                        """,
                        uuid=uuid,
                    )
                    try:
                        raw_node = results.fetch(1)[0]["a"]
                        results.consume()
                    except IndexError:
                        return None
                    node = self.resolve_node(raw_node)
                    print(f"read {node}")
                    return node

    def get_connection_by_uuid(self, uuid: str):
        """
        queries database for the connection by uuid
        """
        with n4.GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()
            with driver.session(database=self.data_base_name) as session:
                with session.begin_transaction() as tx:
                    results: n4.Result = tx.run(
                        """
                        MATCH (a)-[c {uuid: $uuid}]->(b)
                        RETURN c, a, b
                        """,
                        uuid=uuid,
                    )
                    try:
                        res: n4.Result = results.fetch(1)[0]
                        raw_connection = res["c"]
                        results.consume()
                    except IndexError:
                        return None
                    connection = self.resolve_connection(raw_connection)
                    print(f"read {connection}")
                    return connection

    def read(self, query: str, **kwargs):
        """
        query the db and resolve objects
        """
        # TODO implement proxy result that resolves objects
        with n4.GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()
            with driver.session(database=self.data_base_name) as session:
                with session.begin_transaction() as tx:
                    results: n4.Result = tx.run(query, **kwargs)
                    try:
                        # res: n4.Result = results.fetch(1)[0]
                        graph = Graph(self, raw_graph=results.graph())
                    except IndexError:
                        return None
                    return graph

    def read_subgraph(
        self, base_node: Node, with_conns: Set[Connection] = None, max_depth=1
    ) -> Graph:
        """
        Returns a subgraph as JSON.
        The subgraph consists of the `base_node` and all nodes, that are reachable within `max_depth` hops via any connections in `with_conns`
        """

        with_conns = {} if with_conns is None else with_conns

        with n4.GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()
            with driver.session(database=self.data_base_name) as session:
                with session.begin_transaction() as tx:
                    results: n4.Result = tx.run(
                        f"""
                        MATCH (a {{uuid: $uuid}})-[c:{"|".join({x.type for x in with_conns})}]-{{1,{max_depth}}}(b)
                        RETURN a, c, b
                        """,
                        uuid=base_node.uuid,
                    )
                    try:
                        # res: n4.Result = results.fetch(1)[0]
                        graph = Graph(self, raw_graph=results.graph())
                    except IndexError:
                        return None
                    return graph

    def resolve_connection(self, raw_connection: n4_graph.Relationship):
        """
        converts a "raw" neo4j Relationship to an `ogr.Connection`
        """
        self.type_connections_lut[raw_connection.type](
            node_a=self.resolve_node(raw_connection.start_node),
            node_b=self.resolve_node(raw_connection.end_node),
            **raw_connection._properties,
        )

    def resolve_node(self, raw_node: n4_graph.Node):
        """
        converts a "raw" neo4j Node to an `ogr.Node`
        """
        node_type, dyn_labels = self._infer_node_type(raw_node.labels)
        node = node_type(dyn_labels=dyn_labels, **raw_node._properties)
        return node

    def _infer_node_type(self, labels: frozenset) -> (MetaNode, frozenset):
        """
        infers type with maximum label intersection
        """
        # TODO implement basic ogr.Node as fallback

        label_lut_sets = list(self.label_nodes_lut.keys())
        length_set_map = {
            lambda x: len(labels.intersection(x)): x for x in label_lut_sets
        }
        node_type = self.label_nodes_lut[length_set_map[max(length_set_map.keys())]]
        residual = labels.difference(node_type.labels)
        return node_type, residual

    def query(self, query: str, *args, **kwargs):
        with n4.GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()
            with driver.session(database=self.data_base_name) as session:
                with session.begin_transaction() as tx:
                    results: Result = Result(tx.run(query, *args, **kwargs))
                    return results
