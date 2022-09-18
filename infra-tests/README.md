# A testing framework that allows simulation of network failures.

Dynamically configure iptables for nodes of a cluster from a cluster supervisor (exposed via a RESTful api). 
Partial implementation of the Partition API described in [this paper](https://www.scs.stanford.edu/14au-cs244b/labs/projects/RaftMonkey-Chakoumakos-Trusheim-revised.pdf).

# Usage

We'd have a docker image that uses the `aalekhpatel07/draft-test-node:latest` as the base image and has some port running a Raft service (say, a udp server).
The `podman-compose` config can be used to configure a cluster of nodes that will run raft. Any other configuration required to get the rafts to communicate
can be set up before running `podman-compose up -d`.

Then we can talk to the cluster test supervisor based on `aalekhpatel07/draft-test-supervisor:latest` that runs a FastAPI server which lets us execute commands like 
`partition(node1, node2)`. These commands will cause a network partition between the peers with the given ids, etc.

By this API, we should be able to control network partitions and healing within a Raft cluster via an external observer. This is helpful 
to simulate network partitions when testing a Raft implementation.
