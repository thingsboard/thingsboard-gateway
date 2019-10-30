FROM ubuntu:18.04

RUN apt-get update && \
      apt-get -y install sudo
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get install -y tzdata
RUN /usr/bin/apt install python3 python3-dev python3-pip -y
RUN pip3 install thingsboard_gateway

ENV configs /etc/thingsboard-gateway
ENV extensions /var/lib/thingsboard_gateway/extensions
ENV logs /var/log/thingsboard-gateway

VOLUME ["${configs}", "${extensions}", "${logs}"]

CMD /usr/bin/python3 -c "from thingsboard_gateway.tb_gateway import daemon;daemon()"