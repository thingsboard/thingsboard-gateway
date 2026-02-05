%define name thingsboard-gateway
%define version 3.8.2
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
Source4: venv.tar.gz

%description
The ThingsBoard IoT Gateway integrates devices using different protocolos like MQTT, Modbus, OPC-UA and other.

%install

# Install the systemd service file.
mkdir -p %{buildroot}/etc/systemd/system
install -m 644 %{SOURCE0} %{buildroot}/etc/systemd/system/thingsboard-gateway.service

# Install the configuration tarball.
mkdir -p %{buildroot}/etc/thingsboard-gateway
mkdir -p %{buildroot}/etc/thingsboard-gateway/config
tar -xzf %{SOURCE1} -C %{buildroot}/etc/thingsboard-gateway

# Install the wheel file into /var/lib/thingsboard_gateway.
mkdir -p %{buildroot}/var/lib/thingsboard_gateway
install -m 644 %{SOURCE2} %{buildroot}/var/lib/thingsboard_gateway/

# Install extensions into /var/lib/thingsboard_gateway.
install -m 755 %{SOURCE3} %{buildroot}/var/lib/thingsboard_gateway/

# Install venv into /var/lib/thingsboard_gateway.
install -m 755 %{SOURCE4} %{buildroot}/var/lib/thingsboard_gateway/

# Create logs directory.
mkdir -p %{buildroot}/var/log/thingsboard-gateway

# (Ownership is set later via %defattr.)

%pre
getent passwd thingsboard_gateway || useradd -r -U -d /var/lib/thingsboard_gateway -c "ThingsBoard-Gateway Service" thingsboard_gateway

%post
REQUIRED_MAJOR=3
REQUIRED_MINOR=11

show_instruction() {
  echo "To install Python $REQUIRED_MAJOR.$REQUIRED_MINOR:"
  echo ""

  echo "# Step 1: Enable EPEL and IUS repositories (if not already enabled)"
  echo "sudo yum install -y epel-release"
  echo "sudo yum install -y https://repo.ius.io/ius-release-el$(rpm -E %{rhel}).rpm"

  echo ""
  echo "# Step 2: Install Python $REQUIRED_MAJOR.$REQUIRED_MINOR and venv"
  echo "sudo yum install -y python$REQUIRED_MAJOR$REQUIRED_MINOR python$REQUIRED_MAJOR$REQUIRED_MINOR-venv"

  echo ""
  echo "CAUTION: Uninstall previously installed package if install failed"
  echo "sudo rpm -e --noscripts python3-thingsboard-gateway"
}

if [ -f /var/lib/thingsboard_gateway/venv.tar.gz ]; then
  echo "Postinst: Checking python version..."

  PYTHON_BIN=$(command -v python3.11 || true)

  if [ -z "$PYTHON_BIN" ]; then
    echo "Error: python3.11 is not installed." >&2
    exit 0
  fi

  VERSION=$($PYTHON_BIN -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')")
  ACTUAL_MAJOR=$($PYTHON_BIN -c "import sys; print(sys.version_info.major)")
  ACTUAL_MINOR=$($PYTHON_BIN -c "import sys; print(sys.version_info.minor)")

  echo "Detected Python version: $VERSION"

  if [ "$ACTUAL_MAJOR" -ne "$REQUIRED_MAJOR" ] || [ "$ACTUAL_MINOR" -ne "$REQUIRED_MINOR" ]; then
    echo "Error: Required Python version is $REQUIRED_MAJOR.$REQUIRED_MINOR, but found $VERSION" >&2
    show_instruction
    exit 0
  fi

  echo "Python version is compatible."
fi

# Create the Python virtual environment if not present.
if [ -f /var/lib/thingsboard_gateway/venv.tar.gz ]; then
    echo "Postinst: Extracting virtual environment from venv.tar.gz..."
    tar -xzf /var/lib/thingsboard_gateway/venv.tar.gz -C /var/lib/thingsboard_gateway
    rm -f /var/lib/thingsboard_gateway/venv.tar.gz
    /var/lib/thingsboard_gateway/venv/bin/pip install --upgrade /var/lib/thingsboard_gateway/thingsboard_gateway-%{version}-py3-none-any.whl
else
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
fi

# Extract extensions tarball into /var/lib/thingsboard_gateway
if [ -f /var/lib/thingsboard_gateway/extensions.tar.gz ]; then
    if [ ! -d /var/lib/thingsboard_gateway/extensions ]; then
        echo "Extracting extensions from extensions.tar.gz..."
        mkdir -p /var/lib/thingsboard_gateway/extensions
        tar -xzf /var/lib/thingsboard_gateway/extensions.tar.gz -C /var/lib/thingsboard_gateway/extensions
        rm -f /var/lib/thingsboard_gateway/extensions.tar.gz
    else
        echo "Directory /var/lib/thingsboard_gateway/extensions exists. Creating backup as extensions_backup.tar.gz..."
        tar -czf /var/lib/thingsboard_gateway/extensions_backup.tar.gz -C /var/lib/thingsboard_gateway extensions
    fi
fi

# Extract the configuration tarball into /etc/thingsboard-gateway.
if [ -f /etc/thingsboard-gateway/configs.tar.gz ]; then
    if [ ! -d /etc/thingsboard-gateway/config ]; then
        echo "Extracting configuration files from configs.tar.gz..."
        tar -xzf /etc/thingsboard-gateway/configs.tar.gz -C /etc/thingsboard-gateway
    else
        echo "Directory /etc/thingsboard-gateway/config exists. Creating backup as configs_backup.tar.gz..."
        tar -czf /etc/thingsboard-gateway/configs_backup.tar.gz -C /etc/thingsboard-gateway config
    fi
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
%dir %attr(0755,thingsboard_gateway,thingsboard_gateway) /etc/thingsboard-gateway
%config(noreplace) %attr(0644,thingsboard_gateway,thingsboard_gateway) /etc/thingsboard-gateway/config/*
%attr(0755,thingsboard_gateway,thingsboard_gateway) /var/lib/thingsboard_gateway
%attr(0755,thingsboard_gateway,thingsboard_gateway) /var/log/thingsboard-gateway

%postun
if [ "$1" = 0 ]; then
    echo "Cleaning up..."
    systemctl stop thingsboard-gateway
    userdel thingsboard_gateway
    rm -rf /var/lib/thingsboard_gateway
    rm -rf /var/log/thingsboard-gateway
    rm -rf /etc/thingsboard-gateway
fi
