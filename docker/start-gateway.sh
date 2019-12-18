#
#CONF_FOLDER="/etc/thingsboard-gateway/config"
#firstlaunch=${CONG_FOLDER}/.firstlaunch

#if [ ! -f ${firstlaunch} ]; then
#    cp -r /default-config/config/* /etc/thingsboard-gateway/config/
#    cp -r /default-config/extentions/* /var/lib/thingsboard_gateway/extensions/
#    touch ${firstlaunch}
#fi


/usr/bin/python3 -c "from thingsboard_gateway.tb_gateway import daemon; daemon()"
