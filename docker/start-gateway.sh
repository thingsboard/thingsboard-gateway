#
CONF_FOLDER="/etc/thingsboard-gateway/config"
firstlaunch=${CONF_FOLDER}/.firstlaunch

if [ ! -f ${firstlaunch} ]; then
    cp -r /default-config/config/* /etc/thingsboard-gateway/config/
    cp -r /default-config/extensions/* /var/lib/thingsboard_gateway/extensions/
    touch ${firstlaunch}
    echo "#Remove this file only if you want to recreate default config files! This will overwrite exesting files" > ${firstlaunch}
fi

/usr/bin/python3 -c "from thingsboard_gateway.tb_gateway import daemon; daemon()"
