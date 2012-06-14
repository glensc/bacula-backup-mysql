#!/usr/bin/perl -ws
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to:
#
#  Free Software Foundation, Inc.
#  59 Temple Place - Suite 330
#  Boston, MA 02111-1307, USA.

# Rudimentary switch parsing. Must be in main package.
our $cleanup;

package BBM;
use strict;
use POSIX qw(setuid setgid);
use DBI;
use File::Temp qw(tempdir);
use File::Path qw(rmtree);

# path to Apache HTTPd-style config
my $config = '/etc/bacula/backup-mysql.conf';
my $c = new BBM::Config($config);

# now change to user mysql after we've read config
unless ($<) {
	my $uid = getpwnam('mysql');
	my $gid = getgrnam('mysql');
	die "Can't find user/group mysql\n" unless $uid or $gid;

	# CWD could not be accessible for mysql user
	chdir("/");

	$) = "$gid $gid";
	$( = $gid;
	$> = $< = $uid;
}

# setup tmpdir
my $backup_dir = $c->get('options', 'outdir') or die "'outdir' not defined in config\n";
my $tmpdir = $c->get('options', 'tmpdir') or die "'tmpdir' not defined in config\n";

if (!-d $backup_dir && !mkdir($backup_dir) && !-d $backup_dir) {
	die "backup dir '$backup_dir' not present and can't be created\n";
}
if (!-d $tmpdir && !mkdir($tmpdir) && !-d $tmpdir) {
	die "tmpdir '$tmpdir' not present and can't be created\n";
}

# process each cluster
for my $cluster ($c->get('clusters', 'cluster')) {
	print ">>> cluster: $cluster\n";
	if ($cleanup) {
		cleanup_cluster($cluster);
	} else {
		backup_cluster($cluster);
	}
	print "<<< end cluster: $cluster\n";
}

#
# Usage: mysqlhotcopy $CLUSTER $DATABASE $USERNAME $PASSWORD $SOCKET
#
sub mysqlhotcopy {
	my ($cluster, $db, $user, $password, $socket) = @_;

	# strip $database to contain only db name, as the rest of the code assumes $database is just database name
	# i.e: include_database teensForum5./~(phorum_forums|phorum_users)/
	my ($database) = $db =~ /^([^\.]+)/;

	my $dstdir = tempdir("bbm.XXXXXX", DIR => $tmpdir);

	# remove output dir before backup,
	# otherwise the disk space requirement would double
	my $dirname = "$backup_dir/$cluster/$database";
	if (-d $dirname) {
		print ">>>> rmtree $dirname\n";
		rmtree($dirname);
	}

	# make backup with mysqlhotcopy
	my @shell = ('mysqlhotcopy');
	push(@shell, '-u', $user) if $user;
	push(@shell, '-p', $password) if $password;
	push(@shell, '-S', $socket) if $socket;
	push(@shell, $db, $dstdir);
	print ">>>> mysqlhotcopy $database\n";
	system(@shell) == 0 or die "mysqlhotcopy failed: $?\n";

	# put it to "production dir"
	my $cluster_dir = "$backup_dir/$cluster";
	if (!-d $cluster_dir && !mkdir($cluster_dir) && !-d $cluster_dir) {
		rmtree($dstdir);
		die "cluster dir '$cluster_dir' not present and can't be created\n";
	}

	my $srcdir = "$dstdir/$database";
	unless (rename($srcdir, $dirname)) {
		my $err = $!;
		rmtree($dstdir);
		die "Rename '$srcdir'->'$dirname' failed: $err\n";
	}

	rmdir($dstdir) or warn $!;

	print "<<<< mysqlhotcopy $database\n";
}

sub cleanup_cluster {
	my ($cluster) = @_;
	my $cluster_dir = "$backup_dir/$cluster";
	print ">>>> cleanup $cluster_dir\n";
	rmtree($cluster_dir);
	print "<<<< cleanup $cluster_dir\n";
}

sub backup_cluster {
	my ($cluster) = @_;

	# get db connection info
	my $user = $c->get($cluster, 'user') || $c->get('client', 'user');
	my $password = $c->get($cluster, 'password') || $c->get('client', 'password');
	my $socket = $c->get($cluster, 'socket') || $c->get('client', 'socket');

	# get databases to backup
	my @include = $c->get($cluster, 'include_database');
	my @exclude = $c->get($cluster, 'exclude_database');

	# start with include list
	my %dbs = map { $_ => 1 } @include;

	if (@exclude or !@include) {
		my $dbh = new BBM::DB($user, $password, $socket);
		my $sth = $dbh->prepare("show databases");
		$sth->execute();
		while (my($dbname) = $sth->fetchrow_array) {
			next if lc($dbname) eq 'information_schema';
			next if lc($dbname) eq 'performance_schema';
			$dbs{$dbname} = 1;
		}
		undef $dbh;
	}

	# remove excluded databases
	delete @dbs{@exclude};

	# now do the backup
	foreach my $db (keys %dbs) {
		mysqlhotcopy($cluster, $db, $user, $password, $socket);
	}
}

package BBM::DB;
use strict;

# DB class for simple Database connection
sub new {
	my $self = shift;
	my ($user, $password, $socket) = @_;
	my $dsn = '';
	$dsn .= "mysql_socket=$socket" if $socket;
	my $dbh = DBI->connect("DBI:mysql:$dsn", $user, $password, { PrintError => 0, RaiseError => 1 });
	return $dbh;
}

package BBM::Config;
use strict;
use Config::General;

sub new {
	my $self = shift;
	my $class = ref($self) || $self;
	my $file = shift;

	my $config = new Config::General(-ConfigFile => $file, -LowerCaseNames => 1);
	my $this = { $config->getall() };
	bless($this, $class);
}

sub get {
	my ($self, $section, $key) = @_;
	my $h = $self;

	# descend to [cluster] if $section not present in root tree
	unless (exists $h->{$section}) {
		$h = $h->{cluster};
	}

	# pay attention if callee wanted arrays
	return wantarray ? () : undef unless exists $h->{$section};
	return wantarray ? () : undef unless exists $h->{$section}->{$key};

	# deref if wanted array and is arrayref
	return @{$h->{$section}->{$key}} if wantarray && ref $h->{$section}->{$key} eq 'ARRAY';

	return $h->{$section}->{$key};
}
