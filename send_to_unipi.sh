sudo ./generate_deb_package.sh clean

export UNIPI_IP

read -e -p "Enter unipi ip address: " -i "192.168.1.101" UNIPI_IP

scp ./python3-thingsboard-gateway.deb unipi@$UNIPI_IP:/home/unipi/
