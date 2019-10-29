FROM ubuntu:18.04

RUN apt update
RUN apt install python3-pip -y
RUN pip3 install thingsboard_gateway

ENV configs /etc/thingsboard-gateway
ENV extensions /var/lib/thingsboard_gateway/extensions
VOLUME ["${configs}", "${extensions}"]

RUN python3 -c "from thingsboard_gateway.tb_gateway import daemon; daemon()"