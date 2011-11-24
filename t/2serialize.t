#!perl

use strict;
use warnings;
use Digest::MD5 qw(md5_hex);
use Encode qw();
use Messaging::Message;
use POSIX qw(:fcntl_h);
use Test::More;

eval { require Compress::Zlib };

sub contents ($) {
    my($path) = @_;
    my($fh, $contents, $done);

    sysopen($fh, $path, O_RDONLY) or die("cannot sysopen($path): $!\n");
    binmode($fh) or die("cannot binmode($path): $!\n");
    $contents = "";
    $done = -1;
    while ($done) {
	$done = sysread($fh, $contents, 8192, length($contents));
	die("cannot sysread($path): $!\n") unless defined($done);
    }
    close($fh) or die("cannot close($path): $!\n");
    return($contents);
}

sub md5_msg ($) {
    my($msg) = @_;
    my($buf, $tmp, $name, $line);

    # text flag
    $buf = $msg->text() ? "1" : "0";
    # header
    $tmp = "";
    foreach $name (sort(keys(%{ $msg->header() }))) {
	$line = $name . ":" . $msg->header_field($name) . "\n";
	$tmp .= Encode::encode("UTF-8", $line, Encode::FB_CROAK|Encode::LEAVE_SRC);
    }
    $buf .= md5_hex($tmp);
    # body
    if ($msg->text()) {
	$tmp = Encode::encode("UTF-8", $msg->body(), Encode::FB_CROAK|Encode::LEAVE_SRC);
    } else {
	$tmp = $msg->body();
    }
    $buf .= md5_hex($tmp);
    # digest
    return(md5_hex($buf));
}

sub test_one ($) {
    my($path) = @_;
    my($tmp, $msg, $md5);

    die("unexpected path: $path\n")
	unless $path =~ /^(?:.+\/)?([0-9a-f]{32})(\.\d+)?$/;
    $md5 = $1;
    $tmp = contents($path);
    SKIP : {
	skip("Compress::Zlib is not installed", 1)
	    if $tmp =~ /\"encoding\"\s*:\s*\"[a-z0-9\+]*zlib\b/ and
	    not $Compress::Zlib::VERSION;
	eval { $msg = Messaging::Message->deserialize_ref(\$tmp) };
	if ($msg) {
	    is(md5_msg($msg), $md5, $path);
	} else {
	    $@ =~ s/\s*$//;
	    is($@, "", $path);
	}
    }
}

sub test_all (@) {
    plan tests => scalar(@_);
    foreach my $path (@_) {
	test_one($path);
    }
}

if (@ARGV) {
    test_all(@ARGV);
} else {
    test_all(glob("$0.d/*"));
}
