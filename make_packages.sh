#     Copyright 2020. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

CURRENT_VERSION=$( grep -Po 'version = \K(.*)$' setup.cfg )
if [ "$1" = "clean" ] || [ "$1" = "only_clean" ] ; then
  sudo rm -rf /var/log/thingsboard-gateway/
  sudo rm -rf deb_dist/
  sudo rm -rf dist/
  sudo rm -rf thingsboard-gateway.egg-info/
  sudo rm -rf /etc/thingsboard-gateway/
  sudo rm -rf thingsboard-gateway-$CURRENT_VERSION.tar.gz
  sudo rm -rf thingsboard-gateway-$CURRENT_VERSION.deb
  sudo rm -rf python3-thingsboard-gateway.deb
  sudo rm -rf python3-thingsboard-gateway.rpm
  sudo rm -rf thingsboard-gateway-$CURRENT_VERSION.noarch.rpm
  sudo rm -rf /home/zenx/rpmbuild/BUILDROOT/*
  sudo rm -rf build/
  sudo rm -rf docker/config || echo ''
  sudo rm -rf docker/extensions || echo ''
  sudo find thingsboard_gateway/ -name "*.pyc" -exec rm -f {} \;
  sudo apt remove python3-thingsboard-gateway -y
fi


CURRENT_USER=$USER
export PYTHONDONTWRITEBYTECODE=1

if [ "$1" != "only_clean" ] ; then
  echo "Building DEB package"
  # Create sources for DEB package
  python3 setup.py --command-packages=stdeb.command bdist_deb
  sudo find thingsboard_gateway/ -name "*.pyc" -exec rm -f {} \;
  # Adding the files, scripts and permissions
#  sudo sed -i '/^Depends: .*/ s/$/, libffi-dev, libglib2.0-dev, libxml2-dev, libxslt-dev, libssl-dev, zlib1g-dev/' deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/DEBIAN/control >> deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/DEBIAN/control
  sudo cp -r for_build/etc deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway
  sudo cp -r for_build/var deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway
  sudo cp -r -a for_build/DEBIAN deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway
  sudo chown root:root deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/ -R
  sudo chown root:root deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/var/ -R
  sudo chmod 775 deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/DEBIAN/preinst
  sudo chown root:root deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/DEBIAN/preinst
# Bulding Deb package
#  sudo sed -i 's/Build-Depends.*/Build-Depends: python3-setuptools, python3-all, debhelper (>= 7.4.3), libffi-dev, libglib2.0-dev, libxml2-dev, libxslt-dev, libssl-dev, zlib1g-dev, ${python3: Depends}/g' deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/DEBIAN/control >> deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/DEBIAN/control
  dpkg-deb -b deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/
  cp deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway.deb .
  cp python3-thingsboard-gateway.deb docker/
  # Create sources for RPM Package
  echo 'Building RPM package'
  sudo find thingsboard_gateway/ -name "*.pyc" -exec rm -f {} \;
  python3 setup.py bdist_rpm
  sudo find thingsboard_gateway/ -name "*.pyc" -exec rm -f {} \;
  cp build/bdist.linux-x86_64/rpm/* /home/$CURRENT_USER/rpmbuild/ -r
  # Adding the file, scripts and permissions
  cp for_build/etc/systemd/system/thingsboard-gateway.service /home/$CURRENT_USER/rpmbuild/SOURCES/
#  cd for_build/etc/thingsboard-gateway/
  sudo find thingsboard_gateway/ -name "*.pyc" -exec rm -f {} \;
  cp -r thingsboard_gateway/extensions for_build/etc/thingsboard-gateway/
  cd for_build/etc/thingsboard-gateway || echo 0 > /dev/null
  tar -zcvf configs.tar.gz config/*
#  cp -r extensions /home/$CURRENT_USER/rpmbuild/SOURCES/
  tar -zcvf extensions.tar.gz extensions/*
  mv configs.tar.gz ../../../
#  mv extensions.tar.gz /home/$CURRENT_USER/rpmbuild/SOURCES/extensions.tar.gz
  cd ../../../
  rm /home/$CURRENT_USER/rpmbuild/SOURCES/configs.tar.gz
  cp configs.tar.gz /home/$CURRENT_USER/rpmbuild/SOURCES/
  # Bulding RPM Package
  cp thingsboard-gateway.spec /home/$CURRENT_USER/rpmbuild/SPECS/
  rpmbuild -ba thingsboard-gateway.spec
  sudo cp /home/$CURRENT_USER/rpmbuild/RPMS/noarch/*.rpm .
  sudo mv thingsboard-gateway-$CURRENT_VERSION-1.noarch.rpm python3-thingsboard-gateway.rpm
  sudo chown $CURRENT_USER. *.rpm
#  sudo apt install ./deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway.deb -y
#  echo "Building Docker image and container"
#  cp -r for_build/etc/thingsboard-gateway/config docker/
#  cp -r for_build/etc/thingsboard-gateway/extensions docker/
#  cd docker || exit
#  sudo docker build -t thingsboard_gateway .
#  sudo docker run -d -it --mount type=bind,source="$(pwd)""/logs",target=/var/log/thingsboard-gateway -v config:/etc/thingsboard-gateway/config -v extensions:/var/lib/thingsboard_gateway/extensions --name tb_gateway thingsboard_gateway
#  sudo docker ps -a | grep tb_gateway
fi