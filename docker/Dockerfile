FROM ubuntu:18.04

RUN apt-get update && apt-get -y install sudo

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get install -y tzdata wget

#COPY python3-thingsboard-gateway.deb /
RUN wget https://github.com/thingsboard/thingsboard-gateway/releases/latest/download/python3-thingsboard-gateway.deb
RUN sudo apt-get install -y /python3-thingsboard-gateway.deb

RUN echo '#Main start script\n\
CONF_FOLDER="/etc/thingsboard-gateway/config"\n\
firstlaunch=${CONF_FOLDER}/.firstlaunch\n\
\n\
if [ ! -f ${firstlaunch} ]; then\n\
    cp -r /default-config/config/* /etc/thingsboard-gateway/config/\n\
    cp -r /default-config/extensions/* /var/lib/thingsboard_gateway/extensions/\n\
    touch ${firstlaunch}\n\
    echo "#Remove this file only if you want to recreate default config files! This will overwrite exesting files" > ${firstlaunch}\n\
fi\n\
\n\
/usr/bin/python3 -c "from thingsboard_gateway.tb_gateway import daemon; daemon()"\n'\
>> start-gateway.sh

RUN cp start-gateway.sh /usr/bin/
RUN chmod +x /usr/bin/start-gateway.sh

RUN mkdir -p /default-config/config /default-config/extensions/
RUN cp -r /etc/thingsboard-gateway/config/* /default-config/config/
RUN cp -r /var/lib/thingsboard_gateway/extensions/* /default-config/extensions/

ENV configs /etc/thingsboard-gateway/config
ENV extensions /var/lib/thingsboard_gateway/extensions
ENV logs /var/log/thingsboard-gateway

VOLUME ["${configs}", "${extensions}", "${logs}"]

ENTRYPOINT start-gateway.sh