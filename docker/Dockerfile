FROM ubuntu:18.04

RUN apt-get update && apt-get -y install sudo

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get install -y tzdata

COPY python3-thingsboard-gateway.deb /
COPY start-gateway.sh /usr/bin/

RUN chmod +x /usr/bin/start-gateway.sh
RUN sudo apt-get install -y /python3-thingsboard-gateway.deb

ENV configs /etc/thingsboard-gateway/config
ENV extensions /var/lib/thingsboard_gateway/extensions
ENV logs /var/log/thingsboard-gateway

VOLUME ["${configs}", "${extensions}", "${logs}"]

ENTRYPOINT start-gateway.sh