#     Copyright 2019. ThingsBoard
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

if [ "$1" = "clean" ] || [ "$1" = "only_clean" ] ; then
  sudo rm -rf /var/log/thingsboard-gateway/
  sudo rm -rf deb_dist/
  sudo rm -rf dist/
  sudo rm -rf thingsboard-gateway.egg-info/
  sudo rm -rf /etc/thingsboard-gateway/
  sudo rm -rf thingsboard-gateway-2.0.*.tar.gz
  sudo rm -rf /home/zenx/rpmbuild/BUILDROOT/*
  sudo rm -rf build/
  sudo apt remove python3-thingsboard-gateway -y
fi

CURRENT_USER=$USER

if [ "$1" != "only_clean" ] ; then
  echo "Building DEB package"
  # Create sources for DEB package
  python3 setup.py --command-packages=stdeb.command bdist_deb
  # Adding the files, scripts and permissions
  sudo cp -r for_build/etc deb_dist/thingsboard-gateway-2.0.*/debian/python3-thingsboard-gateway
  sudo cp -r for_build/var deb_dist/thingsboard-gateway-2.0.*/debian/python3-thingsboard-gateway
  sudo cp -r -a for_build/DEBIAN deb_dist/thingsboard-gateway-2.0.*/debian/python3-thingsboard-gateway
  sudo chown root:root deb_dist/thingsboard-gateway-2.0.*/debian/python3-thingsboard-gateway/ -R
  sudo chown root:root deb_dist/thingsboard-gateway-2.0.*/debian/python3-thingsboard-gateway/var/ -R
  sudo chmod 775 deb_dist/thingsboard-gateway-2.0.*/debian/python3-thingsboard-gateway/DEBIAN/preinst
  sudo chown root:root deb_dist/thingsboard-gateway-2.0.*/debian/python3-thingsboard-gateway/DEBIAN/preinst
  # Bulding Deb package
  dpkg-deb -b deb_dist/thingsboard-gateway-2.0.*/debian/python3-thingsboard-gateway/
  cp deb_dist/thingsboard-gateway-2.0.*/debian/python3-thingsboard-gateway.deb .
  cp python3-thingsboard-gateway.deb docker/
  # Create sources for RPM Package
  echo 'Building RPM package'
  python3 setup.py bdist_rpm
  cp build/bdist.linux-x86_64/rpm/* /home/$CURRENT_USER/rpmbuild/ -r
  # Adding the file, scripts and permissions
  cp for_build/etc/systemd/system/thingsboard-gateway.service /home/$CURRENT_USER/rpmbuild/SOURCES/
  cd for_build/etc/thingsboard-gateway/
  tar -zcvf configs.tar.gz .*
  cp configs.tar.gz /home/$CURRENT_USER/rpmbuild/SOURCES/
  cd ../../../
  # Bulding RPM Package
#  cp thingsboard-gateway.spec /home/$CURRENT_USER/rpmbuild/SPECS/
  rpmbuild -ba thingsboard-gateway.spec
  cp /home/$CURRENT_USER/rpmbuild/RPMS/noarch/*.rpm .
#  sudo apt install ./deb_dist/thingsboard-gateway-2.0.0/debian/python3-thingsboard-gateway.deb -y
  echo "Building Docker image, container and run it"
  cp -r for_build/etc/thingsboard-gateway/config docker/
  cp -r for_build/etc/thingsboard-gateway/extensions docker/
  cd docker
  sudo docker build -t thingsboard_gateway .
#  sudo docker run -d -it --mount type=bind,source="$(pwd)""/logs",target=/var/log/thingsboard-gateway -v config:/etc/thingsboard-gateway/config -v extensions:/var/lib/thingsboard_gateway/extensions --name tb_gateway thingsboard_gateway
#  sudo docker ps -a | grep tb_gateway
fi
