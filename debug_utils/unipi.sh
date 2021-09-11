git pull 

sudo ./generate_deb_package.sh clean

sudo apt remove python3-thingsboard-gateway

sudo rm -rf /etc/thingsboard-gateway
sudo rm -rf /var/lib/thingsboard_gateway
sudo rm -rf /var/log/thingsboard_gateway

sudo pip3 uninstall thingsboard-gateway
pip3 uninstall thingsboard-gateway

sudo apt install ./python3-thingsboard-gateway.deb


