FROM python:3.9-slim
ADD ./ /
RUN echo '#Main start script\n\
CONF_FOLDER="./thingsboard_gateway/config"\n\
firstlaunch=${CONF_FOLDER}/.firstlaunch\n\
\n\
if [ ! -f ${firstlaunch} ]; then\n\
    cp -r /default-config/config/* /thingsboard_gateway/config/\n\
    cp -r /default-config/extensions/* /thingsboard_gateway/extensions/\n\
    touch ${firstlaunch}\n\
    echo "#Remove this file only if you want to recreate default config files! This will overwrite exesting files" > ${firstlaunch}\n\
fi\n\
echo "nameserver 8.8.8.8" >> /etc/resolv.conf\n\
\n\
python ./thingsboard_gateway/tb_gateway.py\n\
'\
>> start-gateway.sh && chmod +x start-gateway.sh
ENV PATH="/root/.local/bin:$PATH"
ENV configs /thingsboard_gateway/config
ENV extensions /thingsboard_gateway/extensions
ENV logs /thingsboard_gateway/logs
RUN apt-get update && apt-get install gcc python3-dev build-essential protobuf-compiler -y
# The default grpcio version of 1.43.0 was broken on ARM v7
# See this thread: https://groups.google.com/g/grpc-io/c/vjbL3IdZ2Vk?pli=1
RUN pip install grpcio==1.40.0
RUN python3 -m pip install --upgrade pip --user && python3 -m pip install --upgrade setuptools && pip3 install importlib_metadata --user
RUN python /setup.py install && mkdir -p /default-config/config /default-config/extensions/ && cp -r /thingsboard_gateway/config/* /default-config/config/ && cp -r /thingsboard_gateway/extensions/* /default-config/extensions
VOLUME ["${configs}", "${extensions}", "${logs}"]
CMD [ "/bin/sh", "./start-gateway.sh" ]
