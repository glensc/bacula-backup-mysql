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
use warnings;
use Carp;
use POSIX qw(setuid setgid);
use DBI;
use File::Temp qw(tempdir);
use File::Path qw(rmtree);

# http://www.gsp.com/cgi-bin/man.cgi?section=3&topic=sysexits
use constant EX_UNAVAILABLE => 69;
use constant EX_SOFTWARE => 70;

our $VERSION = '0.6.2';

# path to Apache HTTPd-style config
my $configfile = '/etc/bacula/backup-mysql.conf';
my $c = BBM::Config->new($configfile);

# now change to user mysql after we've read config
if (!$<) {
	my $uid = getpwnam('mysql');
	my $gid = getgrnam('mysql');
	croak "Can't find user/group mysql\n" if !$uid || !$gid;

	# CWD could not be accessible for mysql user
	chdir("/");

	$) = "$gid $gid";
	$( = $gid;
	$> = $< = $uid;
}

# setup tmpdir
my $backup_dir = $c->get('options', 'outdir') or croak "'outdir' not defined in config\n";
my $tmpdir = $c->get('options', 'tmpdir') or croak "'tmpdir' not defined in config\n";

if (!-d $backup_dir && !mkdir($backup_dir) && !-d $backup_dir) {
	croak "backup dir '$backup_dir' not present and can't be created\n";
}
if (!-d $tmpdir && !mkdir($tmpdir) && !-d $tmpdir) {
	croak "tmpdir '$tmpdir' not present and can't be created\n";
}

# process each cluster
my $exit;
for my $cluster ($c->get('clusters', 'cluster')) {
	my $rc;
	print ">>> cluster: $cluster\n";
	if ($cleanup) {
		$rc = cleanup_cluster($cluster);
	} else {
		$rc = backup_cluster($cluster);
	}
	# add error code if one was set
	$exit = $rc if !defined($exit) or $rc > $exit;
	print "<<< end cluster: $cluster\n";
}
# if no clusters were processed, exit with UNAVAILABLE status
$exit = EX_UNAVAILABLE unless defined $exit;
exit($exit);

#
# Usage: mysqldump $CLUSTER $DATABASE $TABLES $USERNAME $PASSWORD $SOCKET
#
sub mysqldump {
	my ($cluster, $database, $tables, $user, $password, $socket) = @_;

	my $dstdir = tempdir("bbm.XXXXXX", DIR => $tmpdir);

	# remove output dir before backup,
	# otherwise the disk space requirement would double
	my $dirname = "$backup_dir/$cluster/$database";
	if (-d $dirname) {
		print ">>>> rmtree $dirname\n";
		rmtree($dirname);
	}

	# make backup with mysqldump
	my @shell = ('mysqldump');
	push(@shell, '-u', $user) if $user;
	push(@shell, "-p$password") if $password;
	push(@shell, '-S', $socket) if $socket;
	# use -r option so we don't have to mess with output redirection
	push(@shell, '-r', "$dstdir/mysqldump.sql");

	push(@shell, '--flush-logs');
	# be sure to dump routines as well
	push(@shell, '--routines');
	# single transaction to make snapshot of database
	push(@shell, '--single-transaction');
	# skip dump date, so if the data did not change, the dump will be identical
	push(@shell, '--skip-dump-date');
	# skip drop table so that accidentally loading dump to db already having that table won't destroy your data
	push(@shell, '--skip-add-drop-table');
	# This causes the binary log position and filename to be appended to the output.
	# if equal to 2, that command will be prefixed with a comment symbol.
	push(@shell, '--master-data=2');

	push(@shell, $database, @$tables);
	print ">>>> mysqldump $database\n";
	system(@shell) == 0 or croak "mysqldump failed: $?\n";

	# put it to "production dir"
	my $cluster_dir = "$backup_dir/$cluster";
	if (!-d $cluster_dir && !mkdir($cluster_dir) && !-d $cluster_dir) {
		rmtree($dstdir);
		croak "cluster dir '$cluster_dir' not present and can't be created\n";
	}

	my $srcdir = $dstdir;
	if (!rename($srcdir, $dirname)) {
		my $err = $!;
		rmtree($dstdir);
		croak "Rename '$srcdir'->'$dirname' failed: $err\n";
	}

	print "<<<< mysqldump $database\n";
	return;
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
	push(@shell, '--flushlog');
	push(@shell, '-u', $user) if $user;
	push(@shell, '-p', $password) if $password;
	push(@shell, '-S', $socket) if $socket;

	my $record_log_pos = $c->get($cluster, 'record_log_pos');
	if ($record_log_pos) {
		push(@shell, '--record_log_pos', $record_log_pos);
	}

	push(@shell, $db, $dstdir);
	print ">>>> mysqlhotcopy $database\n";
	my $rc = system(@shell) >> 8;

	# handle exit code 9 specially for empty databases. see issue #3
	if ($rc == 9) {
		$rc = 0;
		print "mysqlhotcopy $database empty\n";
		mkdir("$dstdir/$database");
	} elsif ($rc == 0) {
	} else {
		warn "mysqlhotcopy $database failed: $rc\n";
		return $rc;
	}

	# put it to "production dir"
	my $cluster_dir = "$backup_dir/$cluster";
	if (!-d $cluster_dir && !mkdir($cluster_dir) && !-d $cluster_dir) {
		rmtree($dstdir);
		croak "cluster dir '$cluster_dir' not present and can't be created\n";
	}

	my $srcdir = "$dstdir/$database";
	if (!rename($srcdir, $dirname)) {
		my $err = $!;
		rmtree($dstdir);
		croak "Rename '$srcdir'->'$dirname' failed: $err\n";
	}

	rmdir($dstdir) or carp $!;

	print "<<<< mysqlhotcopy $database\n";
	return $rc;
}

sub cleanup_cluster {
	my ($cluster) = @_;
	my $cluster_dir = "$backup_dir/$cluster";
	print ">>>> cleanup $cluster_dir\n";
	rmtree($cluster_dir);
	print "<<<< cleanup $cluster_dir\n";
	return;
}

sub backup_cluster {
	my ($cluster) = @_;

	# get db connection info
	my $user = $c->get($cluster, 'user') || $c->get('client', 'user');
	my $password = $c->get($cluster, 'password') || $c->get('client', 'password');
	my $socket = $c->get($cluster, 'socket') || $c->get('client', 'socket');

	# dump type: mysqlhotcopy, mysqldump
	my $dump_type = $c->get($cluster,'dump_type') || 'mysqlhotcopy';

	my $dbh = BBM::DB->new($user, $password, $socket);

	# get databases to backup
	my @include = $c->get($cluster, 'include_database');
	my @exclude = $c->get($cluster, 'exclude_database');

	# hash with keys which are database names that we want to backup
	my %dbs = ();

	# split include array into database name and optional table regex
	foreach my $db (@include) {
		my ($dbname, $optional_table_regex) = split(/\./, $db, 2);
		$dbs{$dbname} = $optional_table_regex; # will be undef if it is just a plain database name
	}

	if (@exclude || !@include) {
		my $sth = $dbh->prepare("show databases");
		$sth->execute();
		while (my($dbname) = $sth->fetchrow_array) {
			next if lc($dbname) eq 'information_schema';
			next if lc($dbname) eq 'performance_schema';
			next if exists $dbs{$dbname};
			$dbs{$dbname} = undef;
		}
	}

	# remove excluded databases
	delete @dbs{@exclude};

	my ($exit, $rc);

	# now do the backup
	while (my($db, $regex) = each %dbs) {
		$db = $db . '.' . $regex if $regex;
		if ($dump_type eq 'mysqldump') {
			my ($db, $tables) = BBM::DB::get_backup_tables($dbh, $db);
			eval {
				$rc = mysqldump($cluster, $db, $tables, $user, $password, $socket);
			};
			if ($@) {
				warn "ERROR: $@\n";
				$rc = EX_SOFTWARE;
			}

		} elsif ($dump_type eq 'mysqlhotcopy') {
			my $record_log_pos = $c->get($cluster, 'record_log_pos');
			if ($record_log_pos) {
				# check that the table exists, to give early error
				eval {
					$dbh->do("select ".
						"host, time_stamp, log_file, log_pos, ".
						"master_host, master_log_file, master_log_pos ".
						"from $record_log_pos where 1 != 1"
					);
				};
				croak "Error accessing log_pos table ($record_log_pos): $@" if $@;
			}

			eval {
				$rc = mysqlhotcopy($cluster, $db, $user, $password, $socket);
			};
			if ($@) {
				warn "ERROR: $@\n";
				$rc = EX_SOFTWARE;
			}

		} else {
			croak "Unknown Dump type: $dump_type";
		}

		$exit = $rc if !defined($exit) or $rc > $exit;
	}

	# if nothing was processed, exit with UNAVAILABLE status
	$exit = EX_UNAVAILABLE unless defined $exit;
	return $exit;
}

package BBM::DB;
use strict;

# DB class for simple Database connection
sub new {
	my ($self, $user, $password, $socket) = @_;
	my $dsn = '';
	$dsn .= "mysql_socket=$socket" if $socket;
	my $dbh = DBI->connect("DBI:mysql:$dsn", $user, $password, { PrintError => 0, RaiseError => 1 });
	return $dbh;
}

# get list of tables
# @param DBI::db $dbh
# @param string $db
# @static
sub get_list_of_tables {
	my ($dbh, $db) = @_;

	my $tables = $dbh->selectall_arrayref('SHOW TABLES FROM ' . $dbh->quote_identifier($db));
	my @ignore_tables = ();

	# Ignore tables for the mysql database
	if ($db eq 'mysql') {
		@ignore_tables = qw(general_log slow_log schema apply_status);
	}

	my @res = ();
	if ($#ignore_tables > 1) {
		my @tmp = (map { $_->[0] } @$tables);
		for my $t (@tmp) {
			push(@res, $t) if not exists { map { $_=>1 } @ignore_tables }->{$t};
		}
	} else {
		@res = (map { $_->[0] } @$tables);
	}

	return @res;
}

# process regex from $db (if any) and return tables to be backed up
# it uses similar regex as mysqlhotcopy.
# i.e to dump teensForum5 db without phorum_forums and phorum_users tables:
# include_database teensForum5./~(phorum_forums|phorum_users)/
sub get_backup_tables {
	my ($dbh, $db) = @_;

	my $t_regex = '.*';
	if ($db =~ s{^([^\.]+)\./(.+)/$}{$1}) {
		$t_regex = $2;
	}

	my @dbh_tables = BBM::DB::get_list_of_tables($dbh, $db);

	## generate regex for tables/files
	my $negated = $t_regex =~ s/^~//;   ## note and remove negation operator
	$t_regex = qr/$t_regex/;            ## make regex string from user regex

	## filter (out) tables specified in t_regex
	@dbh_tables = ($negated
					? grep { $_ !~ $t_regex } @dbh_tables
					: grep { $_ =~ $t_regex } @dbh_tables);

	return ($db, \@dbh_tables);
}

package BBM::Config;
use strict;
use Config::General;

sub new {
	my $self = shift;
	my $class = ref($self) || $self;
	my $file = shift;

	my $config = Config::General->new(-ConfigFile => $file, -LowerCaseNames => 1);
	my $this = { $config->getall() };
	return bless($this, $class);
}

sub get {
	my ($self, $section, $key) = @_;
	my $h = $self;

	# descend to [cluster] if $section not present in root tree
	if (!exists $h->{$section}) {
		$h = $h->{cluster};
	}

	# pay attention if callee wanted arrays
	return wantarray ? () : undef if !exists $h->{$section};
	return wantarray ? () : undef if !exists $h->{$section}->{$key};

	# deref if wanted array and is arrayref
	return @{$h->{$section}->{$key}} if wantarray && ref $h->{$section}->{$key} eq 'ARRAY';

	return $h->{$section}->{$key};
}
