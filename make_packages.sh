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

CURRENT_VERSION=$( grep -Po 'VERSION[ ,]=[ ,]\"\K(([0-9])+(\.){0,1})+' setup.py )
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
  sudo rm -rf thingsboard_gateway.egg-info
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
  sudo cp -rL for_build/etc deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway
  sudo cp -r for_build/var deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway
  sudo cp -r -a for_build/DEBIAN deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway
  sudo chown root:root deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/ -R
  sudo chown root:root deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/var/ -R
  sudo chmod 775 deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/DEBIAN/preinst
  sudo chmod +x deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/DEBIAN/postinst
  sudo chown root:root deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/DEBIAN/preinst
# Bulding Deb package
  dpkg-deb -b deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/
  cp deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway.deb .
  # Create sources for RPM Package
  echo 'Building RPM package'
  sudo find thingsboard_gateway/ -name "*.pyc" -exec rm -f {} \;
  python3 setup.py bdist_rpm
  sudo find thingsboard_gateway/ -name "*.pyc" -exec rm -f {} \;
  cp build/bdist.linux-x86_64/rpm/* /home/$CURRENT_USER/rpmbuild/ -r
  # Adding the file, scripts and permissions
  cp for_build/etc/systemd/system/thingsboard-gateway.service /home/$CURRENT_USER/rpmbuild/SOURCES/
  sudo find thingsboard_gateway/ -name "*.pyc" -exec rm -f {} \;
  cp -r thingsboard_gateway/extensions for_build/etc/thingsboard-gateway/
  cd for_build/etc/thingsboard-gateway || echo 0 > /dev/null
  tar -zcvf configs.tar.gz config/*
  tar -zcvf extensions.tar.gz extensions/*
  mv configs.tar.gz ../../../
  cd ../../../
  rm /home/$CURRENT_USER/rpmbuild/SOURCES/configs.tar.gz
  cp configs.tar.gz /home/$CURRENT_USER/rpmbuild/SOURCES/
  # Bulding RPM Package
  cp thingsboard-gateway.spec /home/$CURRENT_USER/rpmbuild/SPECS/
  rpmbuild -ba thingsboard-gateway.spec
  sudo cp /home/$CURRENT_USER/rpmbuild/RPMS/noarch/*.rpm .
  sudo mv thingsboard-gateway-$CURRENT_VERSION-1.noarch.rpm python3-thingsboard-gateway.rpm
  sudo chown $CURRENT_USER. *.rpm
fi
