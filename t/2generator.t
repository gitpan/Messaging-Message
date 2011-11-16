#!perl

use strict;
use warnings;
use Messaging::Message::Generator;
use Test::More tests => 40;

sub test ($) {
    my($mg) = @_;
    my($msg, $idx, $bad, $n, $k, $v);

    foreach $idx (1 .. 10) {
	$bad = "";
	$msg = $mg->message();
	# body
	if ($mg->{"body-content"} eq "base64") {
	    unless ($msg->body() =~ /^[a-zA-Z0-9\+\/]*$/) {
		$bad = "invalid body content";
		goto done;
	    }
	} elsif ($mg->{"body-content"} eq "binary") {
	    # can be anything!
	} elsif ($mg->{"body-content"} eq "index") {
	    unless ($msg->body() =~ /^[0-9 ]*$/) {
		$bad = "invalid body content";
		goto done;
	    }
	} elsif ($mg->{"body-content"} eq "text") {
	    unless ($msg->body() =~ /^[\x20-\x7e]*$/) {
		$bad = "invalid body content";
		goto done;
	    }
	} else {
	    die;
	}
	$n = $mg->{"body-size"};
	if (defined($n)) {
	    if ($n >= 0) {
		unless (length($msg->body()) == $n) {
		    $bad = "invalid body size";
		    goto done;
		}
	    } else {
		unless (length($msg->body()) <= - 2 * $n) {
		    $bad = "invalid body size";
		    goto done;
		}
	    }
	}
	# header
	$n = $mg->{"header-count"};
	if ($n) {
	    if ($n > 0) {
		unless ($msg->header() and keys(%{ $msg->header() }) == $n) {
		    $bad = "invalid header count";
		    goto done;
		}
	    } else {
		unless ($msg->header() and keys(%{ $msg->header() }) <= - 2 * $n) {
		    $bad = "invalid header count";
		    goto done;
		}
	    }
	} else {
	    unless (not $msg->header() or keys(%{ $msg->header() }) == 0) {
		$bad = "invalid header count";
		goto done;
	    }
	}
	$n = $mg->{"header-value-size"};
	if ($msg->header()) {
	    while (($k, $v) = each(%{ $msg->header() })) {
		unless ($k =~ /^[a-zA-Z0-9\-\_]+$/) {
		    $bad = "invalid header key";
		    goto done;
		}
		unless ($v =~ /^[\x20-\x7e]*$/) {
		    $bad = "invalid header value";
		    goto done;
		}
		if ($n > 0) {
 		    unless (length($v) == $n) {
			$bad = "invalid header value size";
			goto done;
		    }
		} else {
 		    unless (length($v) <= - 2 * $n) {
			$bad = "invalid header value size";
			goto done;
		    }
		}
	    }
	}
      done:
	is($bad, "", join(" ", %$mg));
    }
}

test(Messaging::Message::Generator->new());

test(Messaging::Message::Generator->new(
    "body-size" => 99,
    "body-content" => "index",
    "header-count" => 10,
));

test(Messaging::Message::Generator->new(
    "body-size" => 999,
    "body-content" => "base64",
    "header-count" => 10,
    "header-value-size" => 64,
));

test(Messaging::Message::Generator->new(
    "body-size" => -100,
    "body-content" => "text",
    "header-count" => -10,
    "header-value-size" => -40,
));
