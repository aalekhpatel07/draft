from typing import Union

from fastapi import FastAPI
import subprocess
import shlex

from supervisor import (
    Node,
    Edge,
    Graph,
    Supervisor
)


app = FastAPI()


def inv(cmd: str, dry: bool = False):
    full_cmd = f"inv {'--dry ' if dry else ''}" + cmd
    args = shlex.split(full_cmd)
    return subprocess.check_output(args).decode('utf-8')


def build_graph() -> Graph:
    graph = Graph()

    peers = [
        "infra-tests_node1_1",
        "infra-tests_node2_1",
        "infra-tests_node3_1",
    ]
    for (idx, peer) in enumerate(peers):
        graph.add_node(Node(id=idx+1, name=peer, ip=inv(f"get-ip {peer}"), port=9000))

    graph.build_complete_graph()
    return graph

graph: Graph = build_graph()
supe: Supervisor = Supervisor(cluster=graph)


@app.get("/rules/{peer_id}")
async def get_rules(peer_id: int):
    global supe
    
    rules = supe.get_rules_for(peer_id)
    try:
        ip = supe.get_peer_ip(peer_id)
        return {
            "peer_id": peer_id, 
            "peer_ip": ip,
            "rules": rules,
            "success": True
        }
    except ValueError:
        return {
            "peer_id": peer_id, 
            "peer_ip": None,
            "rules": [],
            "success": False
        }


@app.post("/rules/reset/{peer_id}")
async def reset_rules(peer_id: int, dry: Union[bool, None] = False):
    global supe
    ip = supe.get_peer_ip(peer_id)

    inv(f"reset-firewall-services --peers {ip}", dry=dry)

    return {
        "peer_id": peer_id,
        "peer_ip": ip,
        "status": "dry" if dry else "success"
    }

@app.get("/raft/cluster")
async def get_cluster_info():
    global supe

    return supe.as_dict()

@app.post("/raft/partition/{source_peer_id}/{target_peer_id}")
async def partition_cluster(source_peer_id: int, target_peer_id: int):

    global supe

    source_ip = supe.get_peer_ip(source_peer_id)
    target_ip = supe.get_peer_ip(target_peer_id)

    supe.partition(source_peer_id, target_peer_id)


    # inv(f"reset-firewall-services --peers {target_ip}")

    # First, reset firewalld across the cluster.
    # for (peer, ip) in supe.get_ips_for_all().values():
    #     inv(f"reset-firewall-services --peers {ip}")


    return {
        "source": (source_peer_id, source_ip),
        "target": (target_peer_id, target_ip),
        "action": "partition",
        "success": True
    }