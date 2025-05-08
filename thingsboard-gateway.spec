%define name thingsboard-gateway
%define version 3.7.4
%define release 1

Summary: ThingsBoard Gateway for IoT devices.
Name: %{name}
Version: %{version}
Release: %{release}
License: Apache License, Version 2.0
Group: Applications/System
BuildArch: noarch
Vendor: ThingsBoard <info@thingsboard.io>
Url: https://github.com/thingsboard/thingsboard-gateway

# Sources:
Source0: thingsboard-gateway.service
Source1: configs.tar.gz
Source2: thingsboard_gateway-%{version}-py3-none-any.whl
Source3: extensions.tar.gz

%description
The ThingsBoard IoT Gateway integrates devices using different protocolos like MQTT, Modbus, OPC-UA and other.

%install

# Install the systemd service file.
mkdir -p %{buildroot}/etc/systemd/system
install -m 644 %{SOURCE0} %{buildroot}/etc/systemd/system/thingsboard-gateway.service

# Install the configuration tarball.
mkdir -p %{buildroot}/etc/thingsboard-gateway
install -m 755 %{SOURCE1} %{buildroot}/etc/thingsboard-gateway/

# Install the wheel file into /var/lib/thingsboard_gateway.
mkdir -p %{buildroot}/var/lib/thingsboard_gateway
install -m 644 %{SOURCE2} %{buildroot}/var/lib/thingsboard_gateway/

# Install extensions into /var/lib/thingsboard_gateway.
install -m 755 %{SOURCE3} %{buildroot}/var/lib/thingsboard_gateway/

# Create logs directory.
mkdir -p %{buildroot}/var/log/thingsboard-gateway

# (Ownership is set later via %defattr.)

%pre
getent passwd thingsboard_gateway || useradd -r -U -d /var/lib/thingsboard_gateway -c "ThingsBoard-Gateway Service" thingsboard_gateway

%post
# Create the Python virtual environment if not present.
if [ ! -d /var/lib/thingsboard_gateway/venv ]; then
    python3 -m venv /var/lib/thingsboard_gateway/venv
    /var/lib/thingsboard_gateway/venv/bin/pip install --upgrade pip setuptools
fi
# Install the locally built wheel into the venv.
if [ -f /var/lib/thingsboard_gateway/thingsboard_gateway-%{version}-py3-none-any.whl ]; then
    /var/lib/thingsboard_gateway/venv/bin/pip install --upgrade --force-reinstall /var/lib/thingsboard_gateway/thingsboard_gateway-%{version}-py3-none-any.whl
else
    echo "Error: Wheel file not found in /var/lib/thingsboard_gateway" >&2
    exit 1
fi

# Extract extensions tarball into /var/lib/thingsboard_gateway
if [ -f /var/lib/thingsboard_gateway/extensions.tar.gz ]; then
    if [ -d /var/lib/thingsboard_gateway/extensions ]; then
        echo "Directory /var/lib/thingsboard_gateway/extensions exists. Creating backup as extensions_backup.tar.gz..."
        tar -czf /var/lib/thingsboard_gateway/extensions_backup.tar.gz -C /var/lib/thingsboard_gateway extensions
        echo "Removing existing /var/lib/thingsboard_gateway/extensions directory..."
        rm -rf /var/lib/thingsboard_gateway/extensions
    fi
    echo "Extracting extensions from extensions.tar.gz..."
    rm -rf /var/lib/thingsboard_gateway/extensions
    mkdir -p /var/lib/thingsboard_gateway/extensions
    tar -xzf /var/lib/thingsboard_gateway/extensions.tar.gz -C /var/lib/thingsboard_gateway/extensions
fi

# Extract the configuration tarball into /etc/thingsboard-gateway.
if [ -f /etc/thingsboard-gateway/configs.tar.gz ]; then
    if [ -d /etc/thingsboard-gateway/config ]; then
        echo "Directory /etc/thingsboard-gateway/config exists. Creating backup as configs_backup.tar.gz..."
        tar -czf /etc/thingsboard-gateway/configs_backup.tar.gz -C /etc/thingsboard-gateway config
        echo "Removing existing /etc/thingsboard-gateway/config directory..."
        rm -rf /etc/thingsboard-gateway/config
    fi
    echo "Extracting configuration files from configs.tar.gz..."
    tar -xzf /etc/thingsboard-gateway/configs.tar.gz -C /etc/thingsboard-gateway
fi

echo "Setting ownership for directories..."
chown -R thingsboard_gateway:thingsboard_gateway /var/lib/thingsboard_gateway
chown -R thingsboard_gateway:thingsboard_gateway /etc/thingsboard-gateway
chown -R thingsboard_gateway:thingsboard_gateway /var/log/thingsboard-gateway

systemctl enable thingsboard-gateway.service
systemctl start thingsboard-gateway.service

%clean
rm -rf %{buildroot}

%files
%attr(0644,thingsboard_gateway,thingsboard_gateway) /etc/systemd/system/thingsboard-gateway.service
%attr(0755,thingsboard_gateway,thingsboard_gateway) /etc/thingsboard-gateway
%attr(0755,thingsboard_gateway,thingsboard_gateway) /var/lib/thingsboard_gateway
%attr(0755,thingsboard_gateway,thingsboard_gateway) /var/log/thingsboard-gateway


%postun
systemctl stop thingsboard-gateway
userdel thingsboard_gateway
rm -rf /var/lib/thingsboard_gateway
rm -rf /var/log/thingsboard-gateway
rm -rf /etc/thingsboard-gateway
