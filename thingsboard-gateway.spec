%define name thingsboard-gateway
%define version 3.5
%define unmangled_version 3.5
%define release 1

Summary: Thingsboard Gateway for IoT devices.
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}.service
Source1: configs.tar.gz
License: Apache Software License (Apache Software License 2.0)
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: ThingsBoard <info@thingsboard.io>
Url: https://github.com/thingsboard/thingsboard-gateway
Requires(pre): /usr/sbin/useradd, /usr/bin/getent, /usr/bin/pip3, /usr/bin/mkdir, /usr/bin/cp
Requires(post): /usr/bin/systemctl, /usr/bin/cp, /usr/bin/rm, /usr/bin/chown, /usr/bin/sed
Requires(postun): /usr/sbin/userdel, /usr/bin/rm, /usr/bin/systemctl

%description
The Thingsboard IoT Gateway is an open-source solution that allows you to integrate devices connected to legacy and third-party systems with Thingsboard.

%pre
/usr/bin/getent passwd thingsboard_gateway || /usr/sbin/useradd -c "ThingsBoard-Gateway Service" -r -U -d /var/lib/thingsboard_gateway thingsboard_gateway && passwd -d thingsboard_gateway
/usr/bin/pip3 install thingsboard-gateway==%{version}
sudo find $(python3 -c "from thingsboard_gateway import __path__; print(str(__path__[0])+'/extensions')") -name "*.pyc" -exec rm -f {} \;

%build
mkdir -p $RPM_BUILD_ROOT/etc/thingsboard-gateway || echo "ThingsBoard config folder already exists"
mkdir -p $RPM_BUILD_ROOT/var/lib/thingsboard_gateway || echo "ThingsBoard user home directory already exists"
mkdir -p $RPM_BUILD_ROOT/var/lib/thingsboard_gateway/extensions || echo 0 > /dev/null
mkdir -p $RPM_BUILD_ROOT/var/log/thingsboard-gateway || echo "ThingsBoard log directory already exists"
sudo chown -R thingsboard_gateway:thingsboard_gateway $RPM_BUILD_ROOT/var/log/thingsboard-gateway
sudo install -p -D -m 644 %{SOURCE0} $RPM_BUILD_ROOT/etc/systemd/system/thingsboard-gateway.service
sudo install -p -D -m 755 %{SOURCE1} $RPM_BUILD_ROOT/etc/thingsboard-gateway/
sudo /usr/bin/chown thingsboard_gateway:thingsboard_gateway $RPM_BUILD_ROOT/etc/thingsboard-gateway -R
sudo /usr/bin/chown thingsboard_gateway:thingsboard_gateway $RPM_BUILD_ROOT/var/log/thingsboard-gateway -R
sudo /usr/bin/chown thingsboard_gateway:thingsboard_gateway $RPM_BUILD_ROOT/var/lib/thingsboard_gateway -R
# sudo find $(python3 -c "from thingsboard_gateway import __path__; print(str(__path__[0])+'/extensions')") \( -iname '*' ! -iname "*.pyc" \) -exec cp {} $RPM_BUILD_ROOT/var/lib/thingsboard_gateway/extensions \;
# sudo cp -r $(python3 -c "from thingsboard_gateway import __path__; print(str(__path__[0])+'/extensions')") $RPM_BUILD_ROOT/var/lib/thingsboard_gateway/

# %install
# sudo find %{buildroot} -name ".pyc" -delete

%post
/usr/bin/sed -i 's/\.\/logs/\/var\/log\/thingsboard-gateway/g' /etc/thingsboard-gateway/config/logs.json >> /etc/thingsboard-gateway/config/logs.json
/usr/bin/rm -rf $RPM_BUILD_ROOT/etc/thingsboard-gateway/thingsboard-gateway
/usr/bin/rm -f $RPM_BUILD_ROOT/etc/thingsboard-gateway/configs.tar.gz
/usr/bin/systemctl enable thingsboard-gateway.service
/usr/bin/systemctl start thingsboard-gateway.service

%clean
sudo rm -rf $RPM_BUILD_ROOT

%files
/etc/systemd/system/thingsboard-gateway.service
/etc/thingsboard-gateway/
/var/log/thingsboard-gateway/
/var/lib/thingsboard_gateway/
%exclude /usr/local/lib/*
%exclude /usr/local/bin/thingsboard-gateway
%exclude /usr/local/bin/tb-gateway-shell
%exclude /usr/local/bin/tb-gateway-configurator
%defattr(-,thingsboard_gateway,thingsboard_gateway)

%postun
systemctl stop thingsboard-gateway
userdel thingsboard_gateway
/usr/sbin/userdel thingsboard_gateway
/usr/bin/rm -rf $RPM_BUILD_ROOT/var/log/thingsboard-gateway
/usr/bin/rm -rf $RPM_BUILD_ROOT/var/lib/thingsboard_gateway
