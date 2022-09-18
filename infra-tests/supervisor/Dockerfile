FROM python:3.9

RUN apt-get update -y && apt-get upgrade -y

RUN apt-get install -y \
	bat \
	vim \
	iproute2 \
	ssh \
	openssh-client \
	openssh-server \
	sshpass \
	dnsutils

# Remote ssh commands to be run with:
# sshpass -p root ssh root@<container_name> <cmd>
COPY config /root/.ssh/config

WORKDIR /app

COPY . .

RUN mkdir -p /var/venv/supervisor

RUN python3.9 -m venv /var/venv/supervisor/

RUN chmod +x /app/entrypoint.sh

EXPOSE 8000

CMD [ "/bin/bash", "/app/entrypoint.sh" ]