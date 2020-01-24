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
  sudo apt remove python3-thingsboard-gateway -y
fi

if [ "$1" != "only_clean" ] ; then
  echo "Installing libraries for building deb package."
  sudo apt-get install python3-stdeb fakeroot python-all
  echo "Building DEB package"
  echo "Creating sources for DEB package..."
  python3 setup.py --command-packages=stdeb.command bdist_deb
  echo "Adding the files, scripts and permissions in the package"
  sudo cp -r for_build/etc deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway
  sudo cp -r for_build/var deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway
  sudo cp -r -a for_build/DEBIAN deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway
  sudo chown root:root deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/ -R
  sudo chown root:root deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/var/ -R
  sudo chmod 775 deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/DEBIAN/preinst
  sudo chown root:root deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/DEBIAN/preinst
  sudo sed -i '/^Depends: .*/ s/$/, libffi-dev, libglib2.0-dev, libxml2-dev, libxslt-dev, libssl-dev, zlib1g-dev/' deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/DEBIAN/control >> deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/DEBIAN/control
  # Bulding Deb package
  dpkg-deb -b deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway/
  cp deb_dist/thingsboard-gateway-$CURRENT_VERSION/debian/python3-thingsboard-gateway.deb .
fi
