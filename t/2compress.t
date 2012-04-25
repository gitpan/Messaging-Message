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
    is($json->{body}, $str, "$algo body");
}

SKIP : {
    eval { require Compress::LZ4 };
    skip("Compress::LZ4 is not installed", 12) if $@;
    test_compress("lz4", 0, "A"x256, "base64+lz4", "AAEAAB9BAQDnUEFBQUFB");
    test_compress("lz4", 1, "A"x256, "base64+lz4", "AAEAAB9BAQDnUEFBQUFB");
    test_compress("lz4", 0, "\xe8"x256, "base64+lz4", "AAEAAB/oAQDnUOjo6Ojo");
    test_compress("lz4", 1, "\xe8"x256, "base64+lz4+utf8", "AAIAAC/DqAIA/+dQqMOow6g=");
    test_compress("lz4", 0, "ABC"x1023, "base64+lz4", "/QsAAD9BQkMDAP//////////////7VBCQ0FCQw==");
    test_compress("lz4", 1, "ABC"x1023, "base64+lz4", "/QsAAD9BQkMDAP//////////////7VBCQ0FCQw==");
}

SKIP : {
    eval { require Compress::Snappy };
    skip("Compress::Snappy is not installed", 12) if $@;
    test_compress("snappy", 0, "A"x256, "base64+snappy", "gAIAQf4BAP4BAP4BAPoBAA==");
    test_compress("snappy", 1, "A"x256, "base64+snappy", "gAIAQf4BAP4BAP4BAPoBAA==");
    test_compress("snappy", 0, "\xe8"x256, "base64+snappy", "gAIA6P4BAP4BAP4BAPoBAA==");
    test_compress("snappy", 1, "\xe8"x256, "base64+snappy+utf8", "gAQEw6j+AgD+AgD+AgD+AgD+AgD+AgD+AgD2AgA=");
    test_compress("snappy", 0, "ABC"x1023, "base64+snappy", "/RcIQUJD/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA5gMA");
    test_compress("snappy", 1, "ABC"x1023, "base64+snappy", "/RcIQUJD/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA/gMA5gMA");
}

SKIP : {
    eval { require Compress::Zlib };
    skip("Compress::Zlib is not installed", 12) if $@;
    test_compress("zlib", 0, "A"x256, "base64+zlib", "eJxzdBzZAACjYEEB");
    test_compress("zlib", 1, "A"x256, "base64+zlib", "eJxzdBzZAACjYEEB");
    test_compress("zlib", 0, "\xe8"x256, "base64+zlib", "eJx78WJkAwB7zOgB");
    test_compress("zlib", 1, "\xe8"x256, "base64+utf8+zlib", "eJw7vOLwKBzBEADaRWsQ");
    test_compress("zlib", 0, "ABC"x1023, "base64+zlib", "eJztwgENAAAMAqBsav9OD3IY6aKqqj54XswXaA==");
    test_compress("zlib", 1, "ABC"x1023, "base64+zlib", "eJztwgENAAAMAqBsav9OD3IY6aKqqj54XswXaA==");
}
