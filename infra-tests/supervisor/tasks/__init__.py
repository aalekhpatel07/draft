import pdb
from sys import stdout
from invoke import task
from tasks.graph import (
    Graph,
    Node,
    Edge,
    Supervisor
)

PREFIX = "/usr/bin/sshpass -p root ssh root"

def command(cmd: str) -> str:
    return f"{PREFIX}@{{}} {cmd}"


@task(iterable=['peers'])
def list_firewall_services(c, peers=None):

    for peer in peers:
        c.run(
            command("firewall-cmd --list-services").format(peer), 
            pty=True, 
            warn=True
        )

@task(iterable=['peers'])
def reset_firewall_services(c, peers=None):

    for peer in peers:
        c.run(
            command(
                '"for srv in \$(firewall-cmd --list-services); do firewall-cmd --remove-service=\$srv; done"',
            )
            .format(peer),
            pty=True,
            warn=True
        )

        c.run(
            command(
                "firewall-cmd --add-service={{ssh,mdns,dhcpv6-client}}",
            )
            .format(peer),
            pty=True,
            warn=True
        )

        c.run(
            command(
                "firewall-cmd --runtime-to-permanent"
            )
            .format(peer),
            pty=True,
            warn=True
        )

@task
def get_ip(c, peer):
    ip = c.run(
        f"dig {peer} +noall +answer | awk '{{print $5}}'",
        pty=True,
        warn=True,
    )
    return ip.stdout.strip()

# @task
# def build_graph(c):
#     graph = Graph()

#     peers = [
#         "tests_node1_1",
#         "tests_node2_1",
#         "tests_node3_1",
#     ]
#     for (idx, peer) in enumerate(peers):
#         graph.add_node(Node(id=idx+1, name=peer, ip=get_ip(c, peer), port=9000))

#     graph.build_complete_graph()
#     rules = graph.get_rules_for(1)

#     print(rules)
#     return graph
