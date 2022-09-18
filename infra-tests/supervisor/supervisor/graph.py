import typing
from collections import defaultdict


class Node:
    id: int
    name: str
    ip: str
    port: int

    incoming_edges: typing.List['Edge'] = []
    outgoing_edges: typing.List['Edge'] = []

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
        return f"Node(id={self.id}, name={self.name}, ip={self.ip}, port={self.port})"

    def add_incoming_edge(self, edge: 'Edge'):
        self.incoming_edges.append(edge)

    def add_outgoing_edge(self, edge: 'Edge'):
        self.incoming_edges.append(edge)

    def remove_outgoing_edge(self, target: 'Node'):
        maybe_edge = Edge(self, target)
        self.outgoing_edges = [edge for edge in self.outgoing_edges if edge != maybe_edge]
    
    def remove_incoming_edge(self, source: 'Node'):
        maybe_edge = Edge(source, self)
        self.incoming_edges = [edge for edge in self.incoming_edges if edge != maybe_edge]


    def get_rules(self, all_nodes: typing.Iterable['Node']):
        rules = []

        nodes_without_self = set(node for node in all_nodes if node != self)
        incoming_edges_sources = set(edge.source for edge in self.incoming_edges)

        should_drop_for = nodes_without_self.intersection(incoming_edges_sources)
        should_accept_for = nodes_without_self.difference(incoming_edges_sources)

        for node in should_drop_for:
            rules.append(
                f"firewall-cmd --add-rich-rule='rule family=\"ipv4\" source address=\"{node.ip}/32\" port port=\"{node.port}\" protocol=\"udp\" drop'"
            )
        for node in should_accept_for:
            rules.append(
                f"firewall-cmd --add-rich-rule='rule family=\"ipv4\" source address=\"{node.ip}/32\" port port=\"{node.port}\" protocol=\"udp\" accept'"
            )

        return rules

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
        return f"Edge(source={self.source}, target={self.target})"


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

        self.adjacency_matrix[source][target] = True

    def remove_edge(self, source: int, target: int):
        if not all((source in self.nodes, target in self.nodes)):
            return
        edge = Edge(source=self.nodes[source], target=self.nodes[target])

        if edge.id in self.edges:
            del self.edges[edge.id]

        self.adjacency_matrix[source][target] = False

    def get_all_nodes(self):
        return self.nodes.values()

    def get_rules_for(self, node_id: int):
        
        if node_id not in self.nodes:
            return []
        
        node = self.nodes[node_id]
        all_nodes = self.get_all_nodes()

        return node.get_rules(all_nodes=all_nodes)


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


