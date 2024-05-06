from __future__ import annotations
from ogr.model import DataModel
from ogr.graph import Graph

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
# URI = "<URI for Neo4j database>"
# AUTH = ("<Username>", "<Password>")
uri = "neo4j://localhost:7687"
auth = ("neo4j", "12345678")

data_model = DataModel(uri, auth)


@data_model.register_node
class ExampleNode:
    example_node_prop: str


@data_model.register_connection
class ExampleConnection:
    example_connection_prop: str


# TODO proper testing
# TODO proper documentation
# TODO exec and resolve queries (n4 query proxy function)


def test():
    graph = Graph(data_model)
    node1 = ExampleNode(example_node_prop="example_prop_text")
    node2 = ExampleNode(example_node_prop="example_prop_text2")
    node1.create(graph)
    node2.create(graph)
    conn1 = ExampleConnection(
        node_a=node1,
        node_b=node2,
        example_connection_prop="example_prop_text",
    )
    graph.create_connection(conn1)
    data_model.write_graph(
        graph
    )  # TODO think about usability of graph / model functions (which ones should be where)
    node_1_reverse = data_model.get_node_by_uuid(node1.uuid)
    print(node_1_reverse)
    conn_1_reverse = data_model.get_connection_by_uuid(conn1.uuid)
    print(conn_1_reverse)
    print("read_subgraph")
    data_model.read_subgraph(
        base_node=node1, with_conns=[ExampleConnection], max_depth=5
    )


print("start")
test()
print("end")
