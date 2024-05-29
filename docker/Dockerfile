FROM --platform=$TARGETPLATFORM python:3.11-slim AS build

ARG TARGETPLATFORM
ARG BUILDPLATFORM

COPY ./thingsboard_gateway /thingsboard_gateway
COPY . .

ENV PATH="/root/.cargo/bin:/root/.local/bin:$PATH" \
    PYTHONPATH="." \
    configs="/thingsboard_gateway/config" \
    extensions="/thingsboard_gateway/extensions" \
    logs="/thingsboard_gateway/logs"

RUN mkdir -p /default-config/config /default-config/extensions/ &&  \
    cp -r /thingsboard_gateway/config/* /default-config/config/ &&  \
    cp -r /thingsboard_gateway/extensions/* /default-config/extensions &&  \
    echo "Running on $BUILDPLATFORM, building for $TARGETPLATFORM" > /log && \
    apt-get update &&  \
    apt-get install -y --no-install-recommends \
    gcc python3-dev build-essential libssl-dev libffi-dev zlib1g-dev \
    python3-grpcio curl pkg-config libssl-dev &&  \
    case "$TARGETPLATFORM" in \
        "linux/amd64") DEFAULT_HOST="x86_64-unknown-linux-gnu";; \
        "linux/386") DEFAULT_HOST="i686-unknown-linux-gnu";; \
        "linux/arm64") DEFAULT_HOST="aarch64-unknown-linux-gnu";; \
        "linux/arm/v7") DEFAULT_HOST="armv7-unknown-linux-gnueabihf";; \
        *) \
            echo "Unsupported platform detected. Trying to use default value...";; \
        esac && \
    curl https://sh.rustup.rs -sSf | sh -s -- -y --default-host=$DEFAULT_HOST && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* &&  \
    echo '#! /bin/sh\n\
# Main start script\n\
CONF_FOLDER="/thingsboard_gateway/config"\n\
FIRSTLAUNCH="${CONF_FOLDER}/.firstlaunch"\n\
if [ ! -f "$FIRSTLAUNCH" ]; then\n\
    cp -r /default-config/config/* /thingsboard_gateway/config/\n\
    cp -r /default-config/extensions/* /thingsboard_gateway/extensions/\n\
    touch $FIRSTLAUNCH\n\
    echo "#Remove this file only if you want to recreate default config files! This will overwrite existing files" > $FIRSTLAUNCH\n\
fi\n\
echo "nameserver 8.8.8.8" >> /etc/resolv.conf\n\
python /thingsboard_gateway/tb_gateway.py' > /start-gateway.sh && chmod +x /start-gateway.sh &&  \
    python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -m pip install --no-cache-dir -r requirements.txt

VOLUME ["${configs}", "${extensions}", "${logs}"]

CMD [ "/bin/sh", "/start-gateway.sh" ]