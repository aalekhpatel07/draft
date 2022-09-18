FROM fedora

RUN dnf update -y && dnf upgrade -y

RUN dnf install -y \
    firewalld \
    vim \
    curl \
    bat \
    net-tools \
    iproute \
    htop \
    openssh \
    openssh-server \
    openssh-clients \
    passwd

RUN dnf clean all; systemctl enable firewalld;

# So that root has a password of "root".
RUN echo "root" | passwd "root" --stdin

# Set PermitRootLogin to yes. To allow the supervisor to run commands remotely through ssh.
# Needed to dynamically configure firewalld.
COPY sshd_config /etc/ssh/sshd_config

RUN systemctl enable sshd

CMD ["/sbin/init"]
