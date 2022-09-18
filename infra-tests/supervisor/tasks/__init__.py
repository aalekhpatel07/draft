from invoke import task


SSH_PREFIX = "/usr/bin/sshpass -p root ssh root"


def command(cmd: str) -> str:
    """
    Build a command to be run on a peer through ssh.
    """
    return f"{SSH_PREFIX}@{{}} {cmd}"


@task
def get_iptables(c, peer=None):
    """
    Given peer, run the "iptables -L OUTPUT -n" command on it.
    """
    full_cmd = command(
        f"iptables -L OUTPUT -n"
    ).format(peer)

    result = c.run(
        full_cmd,
        pty=True,
        warn=False
    )
    print(result.stdout.strip())
    return

@task
def restore_iptables(c, peer=None):
    """
    Given peer, run the "iptables -F" command on it.
    """
    full_cmd = command(
        f"iptables -F"
    ).format(peer)

    result = c.run(
        full_cmd,
        pty=True,
        warn=True
    )
    print(result.stdout.strip())


@task
def drop_from(c, source_ip=None, target_peer_name=None):
    """
    Given a source_ip, and a target_peer hostname, 
    run the "iptables -A OUTPUT -s <source_ip> -j DROP " 
    command on the target peer.
    """
    full_cmd = command(
        f"iptables -A OUTPUT -s {source_ip} -j DROP"
    ).format(target_peer_name)

    result = c.run(
        full_cmd,
        pty=True,
        warn=True
    )
    print(result.stdout.strip())

@task
def restore_from(c, source_ip=None, target_peer_name=None):
    """
    Given a source_ip, and a target_peer hostname, 
    run the "iptables -D OUTPUT -s <source_ip> -j DROP " 
    command on the target peer.
    """
    full_cmd = command(
        f"iptables -D OUTPUT -s {source_ip} -j DROP"
    ).format(target_peer_name)

    result = c.run(
        full_cmd,
        pty=True,
        warn=True
    )
    print(result.stdout.strip())
