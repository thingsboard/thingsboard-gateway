if [ "$1" = "clean" ] || [ "$1" = "only_clean" ] ; then
  sudo apt remove python3-thingsboard-gateway -y
  sudo rm -rf /var/log/thingsboard-gateway/
  sudo rm -rf deb_dist/
  sudo rm -rf dist/
  sudo rm -rf thingsboard-gateway.egg-info/
  sudo rm -rf /etc/thingsboard-gateway/
  sudo rm -rf thingsboard-gateway-1.0.0.tar.gz
fi

#IFS=':' read -ra env_path <<< "$PATH"


if [ "$1" != "only_clean" ] ; then
  python3 setup.py --command-packages=stdeb.command bdist_deb
  sudo cp -r temp/etc deb_dist/thingsboard-gateway-1.0.0/debian/python3-thingsboard-gateway
  sudo cp -r temp/var deb_dist/thingsboard-gateway-1.0.0/debian/python3-thingsboard-gateway
  sudo cp -r -a temp/DEBIAN deb_dist/thingsboard-gateway-1.0.0/debian/python3-thingsboard-gateway
  sudo chown root:root deb_dist/thingsboard-gateway-1.0.0/debian/python3-thingsboard-gateway/ -R
  sudo chown root:root deb_dist/thingsboard-gateway-1.0.0/debian/python3-thingsboard-gateway/var/ -R
  sudo chmod 775 deb_dist/thingsboard-gateway-1.0.0/debian/python3-thingsboard-gateway/DEBIAN/preinst
  sudo chown root:root deb_dist/thingsboard-gateway-1.0.0/debian/python3-thingsboard-gateway/DEBIAN/preinst
  dpkg-deb -b deb_dist/thingsboard-gateway-1.0.0/debian/python3-thingsboard-gateway/
  cp deb_dist/thingsboard-gateway-1.0.0/debian/python3-thingsboard-gateway.deb .
  python3 setup.py --command-packages=stdeb.command bdist_rpm
#  sudo apt install ./deb_dist/thingsboard-gateway-1.0.0/debian/python3-thingsboard-gateway.deb -y
fi
