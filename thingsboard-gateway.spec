%define name thingsboard-gateway
%define version 2.0.0
%define unmangled_version 2.0.0
%define unmangled_version 2.0.0
%define release 1

Summary: Thingsboard Gateway for IoT devices.
Name: %{name}
Version: %{version}
Release: %{release}
#Source0: %{name}-%{unmangled_version}.tar.gz
Source0: %{name}.service
Source1: configs.tar.gz
License: Apache Software License (Apache Software License 2.0)
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: ThingsBoard <info@thingsboard.io>
Url: https://github.com/thingsboard/thingsboard-gateway
Requires(pre): /usr/sbin/useradd, /usr/bin/getent, /usr/bin/pip3
Requires(post): /usr/bin/systemctl, /usr/bin/cp, /usr/bin/rm, /usr/bin/chown
Requires(postun): /usr/sbin/userdel, /usr/bin/rm, /usr/bin/systemctl

%description
The Thingsboard IoT Gateway is an open-source solution that allows you to integrate devices connected to legacy and third-party systems with Thingsboard.

%pre
/usr/bin/getent passwd thingsboard-gateway || /usr/sbin/useradd -c "ThingsBoard-Gateway Service" -r -U -d /var/lib/thingsboard-gateway thingsboard-gateway && passwd -d thingsboard-gateway
/usr/bin/pip3 install thingsboard_gateway

%build
sudo mkdir -p $RPM_BUILD_ROOT/etc/thingsboard-gateway || echo "ThingsBoard config folder already exists"
sudo mkdir -p $RPM_BUILD_ROOT/var/lib/thingsboard-gateway || echo "ThingsBoard user home directory already exists"
sudo mkdir -p $RPM_BUILD_ROOT/var/log/thingsboard-gateway || echo "ThingsBoard log directory already exists"
sudo chown -R thingsboard-gateway:thingsboard-gateway $RPM_BUILD_ROOT/var/log/thingsboard-gateway
sudo pip3 install thingsboard_gateway
sudo install -p -D -m 644 %{SOURCE0} $RPM_BUILD_ROOT/etc/systemd/system/thingsboard-gateway.service
sudo install -p -D -m 755 %{SOURCE1} $RPM_BUILD_ROOT/etc/thingsboard-gateway/
sudo tar -xvf %{SOURCE1} -C $RPM_BUILD_ROOT/etc/thingsboard-gateway/

%post
/usr/bin/rm -rf $RPM_BUILD_ROOT/etc/thingsboard-gateway/thingsboard-gateway
/usr/bin/rm -f $RPM_BUILD_ROOT/etc/thingsboard-gateway/configs.tar.gz
/usr/bin/chown thingsboard-gateway:thingsboard-gateway $RPM_BUILD_ROOT/etc/thingsboard-gateway -R
/usr/bin/chown thingsboard-gateway:thingsboard-gateway $RPM_BUILD_ROOT/var/log/thingsboard-gateway -R
/usr/bin/systemctl enable thingsboard-gateway.service
/usr/bin/systemctl start thingsboard-gateway.service

%clean
sudo rm -rf $RPM_BUILD_ROOT

%files
/etc/systemd/system/thingsboard-gateway.service
/etc/thingsboard-gateway/
/var/log/thingsboard-gateway
%defattr(-,thingsboard-gateway,thingsboard-gateway)

%postun
/usr/sbin/userdel thingsboard-gateway
/usr/bin/rm -rf $RPM_BUILD_ROOT/var/log/thingsboard-gateway
/usr/bin/rm -rf $RPM_BUILD_ROOT/var/lib/thingsboard-gateway
