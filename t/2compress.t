#!/usr/bin/perl

use strict;
use warnings;
use Messaging::Message;
use Test::More tests => 36;

sub test_compress ($$$$$) {
    my($algo, $text, $body, $encoding, $str) = @_;
    my($msg, $json);

    $msg = Messaging::Message->new(text => $text, body => $body);
    $json = $msg->jsonify(compression => $algo);
    is($json->{encoding}, $encoding, "$algo encoding");
    if ($json->{body} eq $str) {
	# cool, same compressed data
	ok(1, "$algo body (compressed)");
    } else {
	# compressed data mismatch, maybe this is not a problem
	$msg = Messaging::Message->dejsonify($json);
	is($body, $msg->body(), "$algo body (uncompressed)");
    }
}

SKIP : {
    eval("use Compress::LZ4 0.12 qw()");
    skip("recent enough Compress::LZ4 not installed", 12) if $@;
    test_compress("lz4", 0, "A"x256, "base64+lz4", "AAEAAB9BAQDnUEFBQUFB");
    test_compress("lz4", 1, "A"x256, "base64+lz4", "AAEAAB9BAQDnUEFBQUFB");
    test_compress("lz4", 0, "\xe8"x256, "base64+lz4", "AAEAAB/oAQDnUOjo6Ojo");
    test_compress("lz4", 1, "\xe8"x256, "base64+lz4+utf8", "AAIAAC/DqAIA/+dQqMOow6g=");
    test_compress("lz4", 0, "ABC"x1023, "base64+lz4", "/QsAAD9BQkMDAP//////////////7VBCQ0FCQw==");
    test_compress("lz4", 1, "ABC"x1023, "base64+lz4", "/QsAAD9BQkMDAP//////////////7VBCQ0FCQw==");
}

SKIP : {
    eval("use Compress::Snappy 0.17 qw()");
    skip("recent enough Compress::Snappy not installed", 12) if $@;
    test_compress("snappy", 0, "A"x256, "base64+snappy", "gAIAQf4BAP4BAP4BAPYBAABB");
    test_compress("snappy", 1, "A"x256, "base64+snappy", "gAIAQf4BAP4BAP4BAPoBAA==");
    test_compress("snappy", 0, "\xe8"x256, "base64+snappy", "gAIA6P4BAP4BAP4BAPoBAA==");
    test_compress("snappy", 1, "\xe8"x256, "base64+snappy+utf8", "gAQEw6j+AgD+AgD+AgD+AgD+AgD+AgD+AgD2AgA=");
    test_compress("snappy", 0, "ABC"x1023, "base64+snappy", "/RcIQUJD/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA5gMA");
    test_compress("snappy", 1, "ABC"x1023, "base64+snappy", "/RcIQUJD/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA5gMA");
}

SKIP : {
    eval("use Compress::Zlib 2.007 qw()");
    skip("recent enough Compress::Zlib not installed", 12) if $@;
    test_compress("zlib", 0, "A"x256, "base64+zlib", "eJxzdBzZAACjYEEB");
    test_compress("zlib", 1, "A"x256, "base64+zlib", "eJxzdBzZAACjYEEB");
    test_compress("zlib", 0, "\xe8"x256, "base64+zlib", "eJx78WJkAwB7zOgB");
    test_compress("zlib", 1, "\xe8"x256, "base64+utf8+zlib", "eJw7vOLwKBzBEADaRWsQ");
    test_compress("zlib", 0, "ABC"x1023, "base64+zlib", "eJztwgENAAAMAqBsav9OD3IY6aKqqj54XswXaA==");
    test_compress("zlib", 1, "ABC"x1023, "base64+zlib", "eJztwgENAAAMAqBsav9OD3IY6aKqqj54XswXaA==");
}
