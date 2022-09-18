import typing
from collections import defaultdict


class Node:
    id: int
    name: str
    ip: str
    port: int

    def __init__(self, id: int, name: str, port: int, ip: str):
        self.id = id
        self.port = port
        self.name = name
        self.ip = ip

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: 'Node') -> bool:
        return self.id == other.id

    def __str__(self) -> str:
        return str(self.as_dict())

    def as_dict(self) -> typing.Dict[str, typing.Any]:
        return {
            "id": self.id,
            "name": self.name,
            "ip": self.ip,
            "port": self.port
        }

    # def get_rules(self, all_nodes: typing.Iterable['Node']):
    #     rules = []

    #     nodes_without_self = set(node for node in all_nodes if node != self)
    #     incoming_edges_sources = set(edge.source for edge in self.incoming_edges if edge.source != self)

    #     should_accept_for = incoming_edges_sources
    #     should_drop_for = nodes_without_self.difference(should_accept_for)

    #     print(
    #         "should accept for: " + + "should drop for: "
    #     )

    #     for node in should_drop_for:
    #         rules.append(
    #             f"firewall-cmd --add-rich-rule='rule family=\"ipv4\" source address=\"{node.ip}/32\" port port=\"{node.port}\" protocol=\"udp\" drop'"
    #         )
    #     for node in should_accept_for:
    #         rules.append(
    #             f"firewall-cmd --add-rich-rule='rule family=\"ipv4\" source address=\"{node.ip}/32\" port port=\"{node.port}\" protocol=\"udp\" accept'"
    #         )

    #     return rules

class Edge:
    source: Node
    target: Node

    def __init__(self, source: Node, target: Node) -> None:
        self.source = source
        self.target = target

    @property
    def id(self):
        return f"[{self.source.id}-{self.target.id}]"

    def __hash__(self) -> int:
        return hash((self.source, self.target))

    def __str__(self) -> str:
        return str(self.as_dict())

    def as_dict(self) -> typing.Dict[str, typing.Any]:
        return {
            "source": self.source.as_dict(),
            "target": self.target.as_dict(),
        }

    def __eq__(self, other: 'Edge') -> bool:
        return self.id == other.id


class Graph:
    """"""
    nodes: typing.Dict[int, Node]
    edges: typing.Dict[int, Edge]

    adjacency_matrix: typing.DefaultDict[Node, typing.DefaultDict[Node, bool]] = defaultdict(lambda: defaultdict(bool))

    def __init__(self) -> None:
        self.nodes = dict()
        self.edges = dict()
        return

    def build_complete_graph(self) -> None:
        for source in self.nodes:
            for target in self.nodes:
                if source == target:
                    continue
                self.add_edge(source, target)

    def add_node(self, node: Node):
        self.nodes[node.id] = node
    
    def add_edge(self, source: int, target: int):
        if not all((source in self.nodes, target in self.nodes)):
            return

        edge = Edge(source=self.nodes[source], target=self.nodes[target])
        self.edges[edge.id] = edge

        self.adjacency_matrix[self.nodes[source]][self.nodes[target]] = True

    def remove_edge(self, source: int, target: int):
        if not all((source in self.nodes, target in self.nodes)):
            return
        edge = Edge(source=self.nodes[source], target=self.nodes[target])

        if edge.id in self.edges:
            del self.edges[edge.id]

        self.adjacency_matrix[self.nodes[source]][self.nodes[target]] = False

    def get_all_nodes(self):
        return self.nodes.values()

    def get_rules_for(self, node_id: int):
        
        if node_id not in self.nodes:
            return []
        
        # node = self.nodes[node_id]
        all_nodes = self.get_all_nodes()

        incoming_edges_from = set()

        for source_node in self.adjacency_matrix:
            for target_node in self.adjacency_matrix[source_node]:
                if self.adjacency_matrix[source_node][target_node]:
                    if target_node == self.nodes[node_id]:
                        incoming_edges_from.add(source_node)

        print([str(node) for node in incoming_edges_from])

        disallow_nodes = set(all_nodes) - incoming_edges_from - { self.nodes[node_id] }
        
        rules = []

        for node in disallow_nodes:
            rules.append(
                f"firewall-cmd --add-rich-rule='rule family=\"ipv4\" source address=\"{node.ip}/32\" port port=\"{node.port}\" protocol=\"udp\" drop'"
            )
        for node in incoming_edges_from:
            rules.append(
                f"firewall-cmd --add-rich-rule='rule family=\"ipv4\" source address=\"{node.ip}/32\" port port=\"{node.port}\" protocol=\"udp\" accept'"
            )

        return rules

    def as_dict(self):
        return {
            "nodes": [node.as_dict() for node in self.nodes.values()],
            "edges": [edge.as_dict() for edge in self.edges.values()],
            "adjacency_matrix": str(self.adjacency_matrix)
        }
    
    def __str__(self):
        return str(self.as_dict())


class Supervisor:
    """"""

    cluster: Graph

    def __init__(self, cluster) -> None:
        self.cluster = cluster

    def partition(self, source: int, target: int):
        self.cluster.remove_edge(source, target)
        self.cluster.remove_edge(target, source)
        return

    def get_rules_for(self, node: int):
        return self.cluster.get_rules_for(node)


    def get_peer_ip(self, node: int):
        if node in self.cluster.nodes:
            return self.cluster.nodes[node].ip
        raise ValueError(f"No node with id: {node} exists.")
    
    def get_ips_for_all(self):
        return {
            node: self.cluster.nodes[node].ip
            for node in self.cluster.nodes
        }

    def get_rules_for_all(self):
        return {
            node: self.get_rules_for(node)
            for node in self.cluster.nodes
        }

    def as_dict(self):
        return {
            "cluster": self.cluster.as_dict()
        }