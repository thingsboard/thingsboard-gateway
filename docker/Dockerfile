FROM python:3.10-slim
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
ENV PYTHONPATH=.
ENV configs /thingsboard_gateway/config
ENV extensions /thingsboard_gateway/extensions
ENV logs /thingsboard_gateway/logs
ENV CRYPTOGRAPHY_DONT_BUILD_RUST 1
RUN apt-get update && apt-get install gcc python3-dev build-essential protobuf-compiler libssl-dev libffi-dev -y
# The default grpcio version of 1.43.0 was broken on ARM v7
# See this thread: https://groups.google.com/g/grpc-io/c/vjbL3IdZ2Vk?pli=1
RUN mkdir -p /default-config/config /default-config/extensions/ && cp -r /thingsboard_gateway/config/* /default-config/config/ && cp -r /thingsboard_gateway/extensions/* /default-config/extensions
RUN python3 -m pip install --no-cache-dir --upgrade pip --user && python3 -m pip install --no-cache-dir --upgrade setuptools && python3 -m pip install --no-cache-dir importlib_metadata --user && python3 -m pip install --no-cache-dir -r requirements.txt
VOLUME ["${configs}", "${extensions}", "${logs}"]
COPY . .
CMD [ "/bin/sh", "./start-gateway.sh" ]
