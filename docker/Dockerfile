FROM --platform=$BUILDPLATFORM python:3.11-slim AS build
ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "Running on $BUILDPLATFORM, building for $TARGETPLATFORM" > /log
# Update package lists and install necessary dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc python3-dev build-essential libssl-dev libffi-dev zlib1g-dev \
    unixODBC unixODBC-dev odbc-postgresql \
    && apt-get install python3-grpcio -y && rm -rf /var/lib/apt/lists/*

# Copy the application code and configuration
ADD ./ /

# Set up the start script
RUN echo '#Main start script\n\
CONF_FOLDER="./thingsboard_gateway/config"\n\
firstlaunch=${CONF_FOLDER}/.firstlaunch\n\
\n\
if [ ! -f ${firstlaunch} ]; then\n\
    cp -r /default-config/config/* /thingsboard_gateway/config/\n\
    cp -r /default-config/extensions/* /thingsboard_gateway/extensions/\n\
    touch ${firstlaunch}\n\
    echo "#Remove this file only if you want to recreate default config files! This will overwrite existing files" > ${firstlaunch}\n\
fi\n\
echo "nameserver 8.8.8.8" >> /etc/resolv.conf\n\
\n\
python ./thingsboard_gateway/tb_gateway.py\n\
'\
>> start-gateway.sh && chmod +x start-gateway.sh

# Set environment variables
ENV PATH="/root/.local/bin:$PATH"
ENV PYTHONPATH=.
ENV configs /thingsboard_gateway/config
ENV extensions /thingsboard_gateway/extensions
ENV logs /thingsboard_gateway/logs
ENV CRYPTOGRAPHY_DONT_BUILD_RUST 1

RUN mkdir -p /default-config/config /default-config/extensions/ && cp -r /thingsboard_gateway/config/* /default-config/config/ && cp -r /thingsboard_gateway/extensions/* /default-config/extensions

# Install Python packages
COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir --upgrade setuptools && \
    python3 -m pip install --no-cache-dir importlib_metadata && \
    python3 -m pip install --no-cache-dir -r requirements.txt

# Create volume mounts
VOLUME ["${configs}", "${extensions}", "${logs}"]

# Copy the remaining files
COPY . .

# Set the container command
CMD [ "/bin/sh", "./start-gateway.sh" ]