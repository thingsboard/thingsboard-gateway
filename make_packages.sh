#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# -----------------------------------------------------------------------------
# Copyright 2025. ThingsBoard
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This script builds both DEB and RPM packages for the ThingsBoard IoT Gateway.
# It automatically installs required system packages (rpm, zstd, xz-utils, etc.)
# using apt-get if they are not already present.
#
# When run with "clean" or "only_clean", it removes all generated files.
#
# --- Automatically install required system packages ---
REQUIRED_PKGS=("rpm" "zstd" "xz-utils")
for pkg in "${REQUIRED_PKGS[@]}"; do
    if ! dpkg -s "$pkg" >/dev/null 2>&1; then
         echo "Package '$pkg' is not installed. Checking availability..."
         if apt-cache show "$pkg" >/dev/null 2>&1; then
             echo "Installing $pkg..."
             sudo apt-get update && sudo apt-get install -y "$pkg"
         else
             echo "Error: Package '$pkg' not found. Please ensure your repositories are enabled and try again."
             exit 1
         fi
    fi
done

# Extract the current version from thingsboard_gateway/version.py
CURRENT_VERSION=$(grep -Po 'VERSION[ ,]=[ ,]"\K(([0-9])+(\.){0,1})+' thingsboard_gateway/version.py)

# --- Clean Block ---
if [ "${1:-}" = "clean" ] || [ "${1:-}" = "only_clean" ]; then
    for d in "/var/log/thingsboard-gateway/" "/var/lib/thingsboard_gateway/" "/etc/thingsboard-gateway/"; do
      if [ -d "$d" ]; then
          sudo rm -rf "$d"
          echo "Directory $d - removed."
      else
          echo "$d does not exist, skipping..."
      fi
    done

    for d in "deb_dist/" "dist/" "thingsboard-gateway.egg-info" "build/"; do
      if [ -d "$d" ]; then
          sudo rm -rf "$d"
          echo "Directory $d - removed."
      else
          echo "$d does not exist, skipping..."
      fi
    done

    for f in "thingsboard-gateway-${CURRENT_VERSION}.tar.gz" "configs.tar.gz" "thingsboard_gateway.tar.gz"; do
      if [ -f "$f" ]; then
          sudo rm -rf "$f"
          echo "File $f - removed."
      else
          echo "$f file does not exist, skipping..."
      fi
    done

    for f in "thingsboard-gateway-*.deb" "python3-thingsboard-gateway.deb" "python3-thingsboard-gateway.rpm" "thingsboard-gateway-*.noarch.rpm"; do
      if ls $f 1> /dev/null 2>&1; then
          sudo rm -rf $f
          echo "File $f - removed."
      else
          echo "No files matching $f found, skipping..."
      fi
    done

    if compgen -G "thingsboard_gateway-*.whl" > /dev/null; then
        sudo rm -f thingsboard_gateway-*.whl
        echo "File $f - removed."
    else
        echo "No thingsboard_gateway-*.whl files found, skipping..."
    fi

    sudo rm -rf thingsboard_gateway/config/backup || echo "Backup folder not found, skipping..."
    sudo rm -rf docker/config docker/extensions || echo "Docker directories not found, skipping..."
    sudo rm -rf for_build/etc/thingsboard-gateway/*
    sudo rm -rf for_build/var/lib/thingsboard_gateway/*
    sudo find thingsboard_gateway/ -name "*.pyc" -exec rm -f {} \;
    sudo apt remove python3-thingsboard-gateway -y || echo "Package not installed, skipping..."

    echo "All generated files removed."
fi

sudo rm -rf thingsboard_gateway/logs/*

if [ "${1:-}" != "only_clean" ]; then

  CURRENT_USER=$USER
  export PYTHONDONTWRITEBYTECODE=1

  echo "Building DEB package"

  # --- Ensure pip and build module are installed ---
  if ! python3 -m pip --version >/dev/null 2>&1; then
    echo "pip not found. Bootstrapping pip with ensurepip..."
    python3 -m ensurepip --upgrade || { echo "Error: pip bootstrapping failed."; exit 1; }
    python3 -m pip install --upgrade --break-system-packages pip
  fi
  python3 -m pip install --upgrade --break-system-packages build

  # --- Build the wheel package ---
  python3 -m build --no-isolation --wheel --outdir .
  WHEEL_FILE=$(ls | grep -E 'thingsboard_gateway-.*\.whl' | head -n 1)
  echo "Found wheel: $WHEEL_FILE"
  if [ ! -f "$WHEEL_FILE" ]; then
    echo "Error: Wheel file $WHEEL_FILE not found."
    exit 1
  fi

  # Create configs.tar.gz from the thingsboard_gateway/config folder if not present.
if [ ! -f configs.tar.gz ]; then
    echo "Creating configs.tar.gz from the thingsboard_gateway/config folder..."
    TEMP_CONFIG_DIR=$(mktemp -d)
    cp -r thingsboard_gateway/config "$TEMP_CONFIG_DIR/"
    sed -i 's#\./logs/#/var/log/thingsboard-gateway/#g' "$TEMP_CONFIG_DIR/config/logs.json"
    tar -czf configs.tar.gz -C "$TEMP_CONFIG_DIR" config
    rm -rf "$TEMP_CONFIG_DIR"
fi


  # Create extensions.tar.gz from the thingsboard_gateway/extensions folder if not present.
  if [ ! -f extensions.tar.gz ]; then
      echo "Creating extensions.tar.gz from the thingsboard_gateway/extensions folder..."
      tar -czf extensions.tar.gz -C thingsboard_gateway extensions
      ls
      pwd
  fi

  # --- Prepare DEB packaging ---
  if [ -d deb_dist ]; then
    sudo chown -R "$USER":"$USER" deb_dist
  fi
  mkdir -p deb_dist/thingsboard-gateway-"$CURRENT_VERSION"/debian/python3-thingsboard-gateway
  mkdir -p for_build/var/lib
  mkdir -p deb_dist/thingsboard-gateway-"$CURRENT_VERSION"/debian/python3-thingsboard-gateway/DEBIAN

  cat <<EOT > deb_dist/thingsboard-gateway-"$CURRENT_VERSION"/debian/python3-thingsboard-gateway/DEBIAN/control
Package: python3-thingsboard-gateway
Version: $CURRENT_VERSION
Section: python
Priority: optional
Architecture: all
Essential: no
Installed-Size: $(du -ks for_build/var/lib | cut -f1)
Maintainer: ThingsBoard <info@thingsboard.io>
Description: ThingsBoard IoT Gateway
 The ThingsBoard Gateway service for handling MQTT, Modbus, OPC-UA, and other connectors.
Depends: python3, python3-venv
EOT

  mkdir -p for_build/var/lib/thingsboard_gateway
  cp extensions.tar.gz for_build/var/lib/thingsboard_gateway
  mkdir -p for_build/etc/thingsboard-gateway
  cp configs.tar.gz for_build/etc/thingsboard-gateway
  rm -f for_build/var/lib/thingsboard_gateway/thingsboard_gateway-*.whl
  cp -r "$WHEEL_FILE" for_build/var/lib/thingsboard_gateway/"$WHEEL_FILE"
  cp -r for_build/etc deb_dist/thingsboard-gateway-"$CURRENT_VERSION"/debian/python3-thingsboard-gateway
  cp -r for_build/var deb_dist/thingsboard-gateway-"$CURRENT_VERSION"/debian/python3-thingsboard-gateway
  cp -r -a for_build/DEBIAN deb_dist/thingsboard-gateway-"$CURRENT_VERSION"/debian/python3-thingsboard-gateway

  sudo chown -R root:root deb_dist/thingsboard-gateway-"$CURRENT_VERSION"/debian/python3-thingsboard-gateway/
  sudo chown -R root:root deb_dist/thingsboard-gateway-"$CURRENT_VERSION"/debian/python3-thingsboard-gateway/var/
  sudo chmod 775 deb_dist/thingsboard-gateway-"$CURRENT_VERSION"/debian/python3-thingsboard-gateway/DEBIAN/preinst
  sudo chmod +x deb_dist/thingsboard-gateway-"$CURRENT_VERSION"/debian/python3-thingsboard-gateway/DEBIAN/postinst
  sudo chown root:root deb_dist/thingsboard-gateway-"$CURRENT_VERSION"/debian/python3-thingsboard-gateway/DEBIAN/preinst

  dpkg-deb -b deb_dist/thingsboard-gateway-"$CURRENT_VERSION"/debian/python3-thingsboard-gateway/

  mkdir deb-temp
  cd deb-temp
  ar x ../deb_dist/thingsboard-gateway-"$CURRENT_VERSION"/debian/python3-thingsboard-gateway.deb
  zstd -d *.zst || echo "No .zst files found or decompression failed."
  rm -f *.zst
  xz *.tar || echo "No .tar.xz files found or decompression failed."
  ar r ../python3-thingsboard-gateway.deb debian-binary control.tar.xz data.tar.xz
  cd ..
  rm -r deb-temp

  echo "DEB package built successfully."

  ####################################
  # Build RPM Package using rpmbuild
  ####################################
  echo "Building RPM package"

  if ! command -v rpmbuild >/dev/null 2>&1; then
    echo "rpmbuild command not found. Installing rpm package..."
    sudo apt-get update && sudo apt-get install -y rpm
    if ! command -v rpmbuild >/dev/null 2>&1; then
         echo "Error: rpmbuild still not found after installing rpm. Exiting."
         exit 1
    fi
  fi

  # Ensure the rpmbuild directory tree exists.
  for sub in BUILD RPMS SOURCES SPECS SRPMS; do
      mkdir -p ~/rpmbuild/$sub
  done

  # Copy sources to rpmbuild/SOURCES.
  cp for_build/etc/systemd/system/thingsboard-gateway.service ~/rpmbuild/SOURCES/
  cp configs.tar.gz ~/rpmbuild/SOURCES/
  cp extensions.tar.gz ~/rpmbuild/SOURCES/
  cp "$WHEEL_FILE" ~/rpmbuild/SOURCES/

  # Copy the spec file to rpmbuild/SPECS.
  cp thingsboard-gateway.spec ~/rpmbuild/SPECS/

  # Build the RPM.
  rpmbuild -ba ~/rpmbuild/SPECS/thingsboard-gateway.spec

  if ls ~/rpmbuild/RPMS/noarch/*.rpm 1> /dev/null 2>&1; then
      cp ~/rpmbuild/RPMS/noarch/*.rpm .
      mv thingsboard-gateway-"$CURRENT_VERSION"-1.noarch.rpm python3-thingsboard-gateway.rpm
      chown "$CURRENT_USER":"$CURRENT_USER" python3-thingsboard-gateway.rpm
      echo "RPM package built successfully."
  else
      echo "RPM build did not produce any RPM files."
  fi

fi
