%include	/usr/lib/rpm/macros.perl
Summary:	MySQL backup hook for Bacula
Name:		bacula-backup-mysql
Version:	0.2
Release:	1
License:	GPL v2
Group:		Applications/Databases
Source0:	%{name}
Source1:	%{name}.conf
BuildRequires:	rpm-perlprov >= 4.1-13
Requires:	/usr/bin/mysqlhotcopy
Requires:	bacula-common
Requires:	perl-DBD-mysql
BuildArch:	noarch
BuildRoot:	%{tmpdir}/%{name}-%{version}-root-%(id -u -n)

%define		_sysconfdir	/etc/bacula

%description
Bacula - It comes by night and sucks the vital essence from your
computers.

This package contains MySQL backup hook.

%prep
%setup -qcT
cp -a %{SOURCE0} .
cp -a %{SOURCE1} .

%install
rm -rf $RPM_BUILD_ROOT
install -d $RPM_BUILD_ROOT{%{_sbindir},%{_sysconfdir}}
install -p %{name} $RPM_BUILD_ROOT%{_sbindir}
cp -a %{name}.conf $RPM_BUILD_ROOT%{_sysconfdir}/backup-mysql.conf

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(644,root,root,755)
%attr(600,root,root) %config(noreplace) %verify(not md5 mtime size) %{_sysconfdir}/backup-mysql.conf
%attr(755,root,root) %{_sbindir}/bacula-backup-mysql
