#+##############################################################################
#                                                                              #
# File: Messaging/Message/Generator.pm                                         #
#                                                                              #
# Description: versatile message generator                                     #
#                                                                              #
#-##############################################################################

#
# module definition
#

package Messaging::Message::Generator;
use strict;
use warnings;
our $VERSION  = "1.0";
our $REVISION = sprintf("%d.%02d", q$Revision: 1.6 $ =~ /(\d+)\.(\d+)/);

#
# used modules
#

use Digest::MD5 qw();
use Messaging::Message qw(_fatal);
use MIME::Base64 qw(encode_base64);
use Params::Validate qw(validate :types);

#
# global variables
#

our(
    $_MD5,			# the MD5 digester object
);

#
# return a random integer between 0 and 2*size, with a normal distribution
#

sub _rndint ($) {
    my($size) = @_;
    my($rnd);

    # see Irwin-Hall in http://en.wikipedia.org/wiki/Normal_distribution
    $rnd = rand(1) + rand(1) + rand(1) + rand(1) + rand(1) + rand(1) +
	   rand(1) + rand(1) + rand(1) + rand(1) + rand(1) + rand(1);
    return(int($rnd * $size / 6 + 0.5));
}

#
# initialize what is needed for _rndbin() to work
#

sub _rndbin_init () {
    $_MD5 = Digest::MD5->new();
    $_MD5->add($$ . "-" . time());
}

#
# return a random binary string of the given size
#

sub _rndbin ($) {
    my($size) = @_;
    my($data, $digest);

    return("") unless $size > 0;
    $_MD5->add(rand());
    $data = "";
    while (length($data) < $size) {
	$digest = $_MD5->digest();
	$_MD5->add($digest);
	$data .= $digest;
    }
    substr($data, $size) = "";
    return(\$data);
}

#
# return a random text string of the given size (Base64 characters)
#

sub _rndb64 ($) {
    my($size) = @_;
    my($data);

    return("") unless $size > 0;
    $data = encode_base64(${ _rndbin(int($size * 0.75 + 1)) }, "");
    substr($data, $size) = "";
    return(\$data);
}

#
# return a random text string of the given size (printable characters)
#

sub _rndstr ($) {
    my($size) = @_;
    my($data);

    return("") unless $size > 0;
    $data = pack("c*", map(32 + int(rand(95)), 1 .. $size));
    return(\$data);
}

#
# constructor
#

sub new : method {
    my($class, %option, $self);

    $class = shift(@_);
    # check options
    %option = validate(@_, {
	"body-content" => {
	    type => SCALAR, regex => qr/^(base64|binary|index|text)$/, default => "index",
	},
	"body-size" => {
	    type => SCALAR, regex => qr/^-?\d+$/, optional => 1,
	},
	"header-count" => {
	    type => SCALAR, regex => qr/^-?\d+$/, default => 0,
	},
	"header-name-prefix" => {
	    type => SCALAR, default => "rnd-",
	},
	"header-name-size" => {
	    type => SCALAR, regex => qr/^-?\d+$/, default => -16,
	},
	"header-value-size" => {
	    type => SCALAR, regex => qr/^-?\d+$/, default => -32,
	},
    });
    # create generator
    $option{index} = 0;
    $self = \%option;
    # initialize the random source
    _rndbin_init() unless $_MD5;
    # so far so good!
    bless($self, $class);
    return($self);
}

#
# generate a new message
#

sub message : method {
    my($self) = @_;
    my(%option, $size, $what, $count);

    # increment index
    $self->{index}++;
    # generate body
    $size = $self->{"body-size"};
    $size = _rndint(-$size) if $size and $size < 0;
    $what = $self->{"body-content"};
    if (defined($size)) {
	# defined size
	if ($size == 0) {
	    # default is empty body...
	} elsif ($what eq "base64") {
	    $option{body_ref} = _rndb64($size);
	    $option{text} = 1;
	} elsif ($what eq "text") {
	    $option{body_ref} = _rndstr($size);
	    $option{text} = 1;
	} elsif ($what eq "binary") {
	    $option{body_ref} = _rndbin($size);
	    $option{text} = 0;
	} elsif ($what eq "index") {
	    if ($size < length($self->{index})) {
		$option{body} = substr($self->{index}, 0, $size);
	    } elsif ($size > length($self->{index})) {
		$count = int(($size + 1) / (length($self->{index}) + 1)) + 1;
		$option{body} = substr(join(" ", ($self->{index}) x $count), 0, $size);
	    } else {
		$option{body} = $self->{index};
	    }
	    $option{text} = 1;
	} else {
	    _fatal("unexpected body-content: %s", $what);
	    return();
	}
    } else {
	# undefined size
	$option{body} = $self->{index} if $what eq "index";
    }
    # generate header
    $count = $self->{"header-count"};
    $count = _rndint(-$count) if $count and $count < 0;
    if ($count) {
	$option{header} = {};
	foreach (1 .. $count) {
	    # name
	    $size = $self->{"header-name-size"};
	    $size = _rndint(-$size) if $size and $size < 0;
	    next unless $size;
	    $what = ${ _rndb64($size) };
	    $what =~ tr[+/][-_];
	    # value
	    $size = $self->{"header-value-size"};
	    $size = _rndint(-$size) if $size and $size < 0;
	    next unless $size;
	    $option{header}{$self->{"header-name-prefix"} . $what} = ${ _rndstr($size) };
	}
    }
    return(Messaging::Message->new(%option));
}

1;

__DATA__

=head1 NAME

Messaging::Message::Generator - versatile message generator

=head1 SYNOPSIS

  use Messaging::Message::Generator;

  # create the generator
  $mg = Messaging::Message::Generator->new(
      "body-content" => "binary",
      "body-size" => 1024,
  );

  # use it to generate 10 messages
  foreach (1 .. 10) {
      $msg = $mg->message();
      ... do something with it ...
  }

=head1 DESCRIPTION

This module provides a versatile message generator that can be useful
for stress testing or benchmarking messaging brokers or libraries.

=head1 METHODS

The following methods are available:

=over

=item new([OPTIONS])

return a new Messaging::Message::Generator object (class method)

=item message()

return a newly generated Messaging::Message object

=back

=head1 OPTIONS

When creating a message generator, the following options can be given:

=over

=item body-content

string specifying the body content type; depending on this value, the
body will be made of:

=over

=item base64

only Base64 characters

=item binary

anything

=item index

the message index number, starting at 1, optionally adjusted to match
the C<body-size> (this is the default)

=item text

only printable 7-bit ASCII characters

=back

=item body-size

integer specifying the body size

=item header-count

integer specifying the number of header fields

=item header-value-size

integer specifying the size of each header field value
(default is -32)

=item header-name-size

integer specifying the size of each header field name
(default is -16)

=item header-name-prefix

string to prepend to all header field names
(default is C<rnd->)

=back

Note: all integer options can be either positive (meaning exactly this
value) or negative (meaning randomly distributed around the value).

For instance:

  $mg = Messaging::Message::Generator->new(
      "header-count" => 10,
      "header-value-size" => -20,
  );

will generate messages with exactly 10 random header fields, each
field value having a random size between 0 and 40 and normally
distributed around 20.

=head1 SEE ALSO

L<Messaging::Message>.

=head1 AUTHOR

Lionel Cons L<http://cern.ch/lionel.cons>

Copyright CERN 2011-2012
