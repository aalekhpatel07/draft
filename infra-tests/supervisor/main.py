import typing

from fastapi import FastAPI
import subprocess
import shlex
import socket
from collections import namedtuple

PEERS = [
    ("infra-tests_node1_1", 1),
    ("infra-tests_node2_1", 2),
    ("infra-tests_node3_1", 3),
]

DESCRIPTION = """
Implementation of a Partition API used to simulate network partitions across a cluster. 

Suppose we have a cluster of 3 nodes with ids (1, 2, 3) and each instance running as a podman
container inside a podman network. Then we can simulate network link breakages/heals between the nodes
in the cluster by using this API.
"""


Node = namedtuple('Node', ['ip', 'hostname'])


class Supervisor:
    """"""

    peers: typing.Dict[int, Node] = dict()

    def __init__(self, peers: typing.Dict[int, typing.Tuple[str, str]]) -> None:
        self.peers = {
            peer: Node(ip=value[1], hostname=value[0])
            for (peer, value) in peers.items()
        }

    def get_peer(self, peer_id: int) -> Node:
        return self.peers[peer_id]

    def get_rules_for(self, node: int):
        return []





app = FastAPI(
    title="Partition API",
    description=DESCRIPTION
)


def inv(cmd: str, dry: bool = False):
    full_cmd = f"inv {'--dry ' if dry else ''}" + cmd
    args = shlex.split(full_cmd)
    return subprocess.check_output(args).decode('utf-8')


def build_peer_map() -> typing.Dict[int, Node]:
    """
    Assuming fixed peers, construct a map of peer_id to its hostname, and ip pair.
    This is just a helper utility.
    """
    global PEERS

    result = dict()
    for (hostname, peer_id) in PEERS:
        ip_addr = socket.gethostbyname(hostname)
        result[peer_id] = Node(hostname=hostname, ip=ip_addr)
    return result


supe: Supervisor = Supervisor(peers=build_peer_map())


@app.post("/partition/{source_peer}/{target_peer}")
async def partition(source_peer: int, target_peer: int):
    """
    Severe the connection between the two given nodes.
    Arguments:
        source_peer: The node to block receiving packets from.
        target_peer: The node that should block receiving the packets.
    """
    global supe

    source = supe.get_peer(source_peer)
    target = supe.get_peer(target_peer)    

    inv(f"drop-from --source-ip {source.ip} --target-peer-name {target.hostname}")

    return {
        "source": supe.get_peer(source_peer),
        "target": supe.get_peer(target_peer),
        "action": "partition"
    }


@app.post("/heal/{source_peer}/{target_peer}")
async def heal(source_peer: int, target_peer: int):
    """
    Heal the connection between the two given nodes.
    Arguments:
        source_peer: The node to unblock receiving packets from.
        target_peer: The node that should unblock receiving the packets.
    """

    global supe

    source = supe.get_peer(source_peer)
    target = supe.get_peer(target_peer)    

    inv(f"restore-from --source-ip {source.ip} --target-peer-name {target.hostname}")

    return {
        "source": supe.get_peer(source_peer),
        "target": supe.get_peer(target_peer),
        "action": "heal"
    }

@app.get("/rules/{peer_id}")
async def get_ip_tables(peer_id: int):
    """
    Get the current status of the "iptables -L OUTPUT -n" command on the given peer.
    """
    global supe

    peer = supe.get_peer(peer_id)

    result = inv(f"get-iptables --peer {peer.hostname}")

    return {
        "peer": peer,
        "output": result
    }

@app.post("/restore")
async def restore():
    """
    Run "iptables -F" on all the nodes. Drops all block rules so that all connections are healed.
    """
    global supe

    peers = [peer for peer in supe.peers.values()]
    result = []

    for peer in peers:
        output = inv(f"restore-iptables --peer {peer.hostname}")
        result.append({
            "peer": peer,
            "output": output
        })

    return result
    