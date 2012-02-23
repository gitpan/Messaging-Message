#+##############################################################################
#                                                                              #
# File: Messaging/Message.pm                                                   #
#                                                                              #
# Description: abstraction of a message                                        #
#                                                                              #
#-##############################################################################

#
# module definition
#

package Messaging::Message;
use strict;
use warnings;
our $VERSION  = "0.9";
our $REVISION = sprintf("%d.%02d", q$Revision: 1.13 $ =~ /(\d+)\.(\d+)/);

#
# export control
#

use Exporter;
our(@ISA, @EXPORT, @EXPORT_OK);
@ISA = qw(Exporter);
@EXPORT = qw();
@EXPORT_OK = qw(_fatal _require);

#
# used modules
#

use Encode qw();
use JSON qw();
use MIME::Base64 qw(encode_base64 decode_base64);
use Params::Validate qw(validate validate_pos :types);

#
# global variables
#

our(
    %_LoadedModule,		# hash of successfully loaded modules
    %_ValidateSpec,		# specifications for validate()
    %_ValidateType,		# types for validate()
    $_JSON,			# JSON object
);

$_JSON = JSON->new();

#
# types
#

$_ValidateType{header} = {
    type => HASHREF,
    callbacks => {
	"hash of strings" => sub { grep(!defined($_)||ref($_), values(%{$_[0]})) == 0 },
    },
};

$_ValidateType{json_bool} = {
    type => OBJECT,
    callbacks => {
	"JSON::is_bool" => sub { JSON::is_bool($_[0]) },
    },
};

#+++############################################################################
#                                                                              #
# helper functions                                                             #
#                                                                              #
#---############################################################################

#
# report a fatal error with a sprintf() API
#

sub _fatal ($@) {
    my($message, @arguments) = @_;

    $message = sprintf($message, @arguments) if @arguments;
    $message =~ s/\s+$//;
    die(caller() . ": $message\n");
}

#
# make sure a module is loaded
#

sub _require ($) {
    my($module) = @_;

    return if $_LoadedModule{$module};
    eval("require $module");
    if ($@) {
	$@ =~ s/\s+at\s.+?\sline\s+\d+\.?$//;
	_fatal("failed to load %s: %s", $module, $@);
    } else {
	$_LoadedModule{$module} = 1;
    }
}

#
# evaluate some code with fatal warnings
#

sub _eval ($&@) {
    my($what, $code);

    $what = shift(@_);
    $code = shift(@_);
    eval {
	local $^W = 1;
	local $SIG{__WARN__} = sub { die($_[0]) };
	&$code;
    };
    return unless $@;
    $@ =~ s/\s+at\s.+?\sline\s+\d+\.?$//;
    _fatal("%s failed: %s", $what, $@);
}

#
# helpers for body encoding and compression
#

sub _maybe_base64_encode ($) {
    my($object) = @_;

    return unless $object->{body} =~ /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\xff]/;
    # only if it contains more than printable ASCII characters (plus \t \n \r)
    _eval("Base64 encoding", sub {
	$object->{body} = encode_base64($object->{body}, "");
    });
    $object->{encoding}{base64}++;
}

sub _maybe_utf8_encode ($) {
    my($object) = @_;
    my($tmp);

    return unless $object->{body} =~ /[\x80-\xff]/;
    # only if it contains more than ASCII characters...
    _eval("UTF-8 encoding", sub {
	$tmp = Encode::encode("UTF-8", $object->{body}, Encode::FB_CROAK|Encode::LEAVE_SRC);
    });
    return if $object->{body} eq $tmp;
    # ... and is worth encoding
    $object->{body} = $tmp;
    $object->{encoding}{utf8}++;
}

sub _maybe_zlib_compress ($) {
    my($object) = @_;
    my($len, $tmp);

    $len = length($object->{body});
    return unless $len > 255;
    # only if it is long enough...
    _eval("Zlib compression", sub {
	$tmp = Compress::Zlib::compress(\$object->{body});
    });
    # FIXME: for text body, we may loose space because of utf+base64... check final length?
    return unless (length($tmp) / $len) < 0.9;
    # ... and is worth compressing
    $object->{body} = $tmp;
    $object->{encoding}{zlib}++;
}

#+++############################################################################
#                                                                              #
# object oriented interface                                                    #
#                                                                              #
#---############################################################################

#
# normal constructor
#

$_ValidateSpec{new} = {
    header   => { %{ $_ValidateType{header} }, optional => 1 },
    body     => { type => SCALAR,              optional => 1 },
    body_ref => { type => SCALARREF,           optional => 1 },
    text     => { type => BOOLEAN,             optional => 1 },
};

sub new : method {
    my($class, %option, $body, $self);

    $class = shift(@_);
    %option = validate(@_, $_ValidateSpec{new}) if @_;
    _fatal("new(): options body and body_ref are mutually exclusive")
	if exists($option{body}) and exists($option{body_ref});
    # default message
    $body = "";
    $self = { header => {}, body_ref => \$body, text => 0 };
    # handle options
    $self->{header} = $option{header}     if exists($option{header});
    $self->{body_ref} = $option{body_ref} if exists($option{body_ref});
    $self->{body_ref} = \$option{body}    if exists($option{body});
    $self->{text} = $option{text} ? 1 : 0 if exists($option{text});
    # so far so good!
    bless($self, $class);
    return($self);
}

#
# normal accessors
#

sub header : method {
    my($self);

    $self = shift(@_);
    return($self->{header}) if @_ == 0;
    validate_pos(@_, { %{ $_ValidateType{header} }, optional => 1 });
    $self->{header} = $_[0];
    return($self);
}

sub body_ref : method {
    my($self);

    $self = shift(@_);
    return($self->{body_ref}) if @_ == 0;
    validate_pos(@_, { type => SCALARREF, optional => 1 })
	unless @_ == 1 and defined($_[0]) and ref($_[0]) eq "SCALAR";
    $self->{body_ref} = $_[0];
    return($self);
}

sub text : method {
    my($self);

    $self = shift(@_);
    return($self->{text}) if @_ == 0;
    validate_pos(@_, { type => BOOLEAN, optional => 1 })
	unless @_ == 1 and (not defined($_[0]) or ref($_[0]) eq "");
    $self->{text} = $_[0] ? 1 : 0;
    return($self);
}

#
# extra accessors
#

sub header_field : method {
    my($self);

    $self = shift(@_);
    if (@_ >= 1 and defined($_[0]) and ref($_[0]) eq "") {
	return($self->{header}{$_[0]}) if @_ == 1;
	if (@_ == 2 and defined($_[1]) and ref($_[1]) eq "") {
	    $self->{header}{$_[0]} = $_[1];
	    return($self);
	}
    }
    # so far so bad :-(
    validate_pos(@_, { type => SCALAR }, { type => SCALAR, optional => 1 });
}

sub body : method {
    my($self, $body);

    $self = shift(@_);
    return(${ $self->{body_ref} }) if @_ == 0;
    validate_pos(@_, { type => SCALAR, optional => 1 })
	unless @_ == 1 and defined($_[0]) and ref($_[0]) eq "";
    $body = $_[0]; # copy
    $self->{body_ref} = \$body;
    return($self);
}

#
# extra methods
#

sub copy : method {
    my($self, %header, $body, $copy);

    $self = shift(@_);
    validate_pos(@_) if @_;
    %header = %{ $self->{header} }; # copy
    $body = ${ $self->{body_ref} }; # copy
    $copy = { header => \%header, body_ref => \$body, text => $self->{text} };
    bless($copy, ref($self));
    return($copy);
}

sub size : method {
    my($self, $size, $key, $value);

    $self = shift(@_);
    validate_pos(@_) if @_;
    $size = 1 + length(${ $self->{body_ref} });
    while (($key, $value) = each(%{ $self->{header} })) {
	$size += 2 + length($key) + length($value);
    }
    return($size);
}

#+++############################################################################
#                                                                              #
# (de)jsonification                                                            #
#                                                                              #
#---############################################################################

#
# jsonify (= transform into a JSON object)
#

$_ValidateSpec{jsonify} = {
    compression => { type => SCALAR, regex => qr/^(zlib)?$/, optional => 1 },
};

sub jsonify : method {
    my($self, %option, $compression, %object);

    $self = shift(@_);
    %option = validate(@_, $_ValidateSpec{jsonify}) if @_;
    $compression = $option{compression} || "";
    # check compression availability
    _require("Compress::Zlib") if $compression eq "zlib";
    # build the JSON object
    $object{text} = JSON::true if $self->{text};
    $object{header} = $self->{header} if keys(%{ $self->{header} });
    return(\%object) unless length(${ $self->{body_ref} });
    $object{body} = ${ $self->{body_ref} };
    # handle non-empty body
    if ($self->{text}) {
	# text body
	if ($compression) {
	    _maybe_utf8_encode(\%object);
	    if ($compression eq "zlib") {
		_maybe_zlib_compress(\%object);
	    }
	    if ($object{encoding} and $object{encoding}{zlib}) {
		# we did compress
		_maybe_base64_encode(\%object);
	    } else {
		# in fact, we did not compress
		$object{body} = ${ $self->{body_ref} };
		delete($object{encoding});
	    }
	}
    } else {
	# binary body
	if ($compression eq "zlib") {
	    _maybe_zlib_compress(\%object);
	}
	_maybe_base64_encode(\%object);
    }
    # set the encoding string
    $object{encoding} = join("+", sort(keys(%{ $object{encoding} })))
	if $object{encoding};
    # so far so good!
    return(\%object);
}

#
# dejsonify (= alternate constructor using the JSON object)
#

$_ValidateSpec{dejsonify} = {
    header   => { %{ $_ValidateType{header} },    optional => 1 },
    body     => { type => SCALAR,                 optional => 1 },
    text     => { %{ $_ValidateType{json_bool} }, optional => 1 },
    encoding => { type => SCALAR,                 optional => 1 },
};

sub dejsonify : method {
    my($class, $object, $encoding, $self, $tmp, $len);

    $class = shift(@_);
    validate_pos(@_, { type => HASHREF })
	unless @_ == 1 and defined($_[0]) and ref($_[0]) eq "HASH";
    validate(@_, $_ValidateSpec{dejsonify});
    $object = $_[0];
    $encoding = $object->{encoding} || "";
    _fatal("invalid encoding: %s", $encoding)
	unless $encoding eq "" or "${encoding}+" =~ /^((base64|utf8|zlib)\+)+$/;
    _require("Compress::Zlib") if $encoding =~ /zlib/;
    # construct the message
    $self = $class->new();
    $self->{text} = 1 if $object->{text};
    $self->{header} = $object->{header} if $object->{header} and keys(%{ $object->{header} });
    if (exists($object->{body})) {
	$tmp = $object->{body};
	if ($encoding =~ /base64/) {
	    # body has been Base64 encoded, compute length to detect unexpected characters
	    # (this is because MIME::Base64 silently ignores them)
	    $len = length($tmp);
	    _fatal("invalid Base64 data: %s", $object->{body}) if $len % 4;
	    $len = $len * 3 / 4;
	    $len -= substr($tmp, -2) =~ tr/=/=/;
	    _eval("Base64 decoding", sub {
		$tmp = decode_base64($tmp);
	    });
	    _fatal("invalid Base64 data: %s", $object->{body}) unless $len == length($tmp);
	}
	if ($encoding =~ /zlib/) {
	    # body has been Zlib compressed
	    _eval("Zlib decompression", sub {
		$tmp = Compress::Zlib::uncompress(\$tmp);
	    });
	}
	if ($encoding =~ /utf8/) {
	    # body has been UTF-8 encoded
	    _eval("UTF-8 decoding", sub {
		$tmp = Encode::decode("UTF-8", $tmp, Encode::FB_CROAK|Encode::LEAVE_SRC);
	    }) if $tmp =~ /[\x80-\xff]/;
	}
	$self->{body_ref} = \$tmp;
    }
    # so far so good!
    return($self);
}

#+++############################################################################
#                                                                              #
# (de)stringification                                                          #
#                                                                              #
#---############################################################################

#
# stringify (= transform into a text string)
#

sub stringify : method {
    my($self, $tmp);

    $self = shift(@_);
    $tmp = $self->jsonify(@_);
    _eval("JSON encoding", sub {
	$tmp = $_JSON->encode($tmp);
    });
    return($tmp);
}

sub stringify_ref : method {
    my($self, $tmp);

    $self = shift(@_);
    $tmp = $self->jsonify(@_);
    _eval("JSON encoding", sub {
	$tmp = $_JSON->encode($tmp);
    });
    return(\$tmp);
}

#
# destringify (= alternate constructor using the stringified representation)
#

sub destringify : method {
    my($class, $tmp);

    $class = shift(@_);
    validate_pos(@_, { type => SCALAR })
	unless @_ == 1 and defined($_[0]) and ref($_[0]) eq "";
    _eval("JSON decoding", sub {
	$tmp = $_JSON->decode($_[0]);
    }, @_);
    return($class->dejsonify($tmp));
}

sub destringify_ref : method {
    my($class, $tmp);

    $class = shift(@_);
    validate_pos(@_, { type => SCALARREF })
	unless @_ == 1 and defined($_[0]) and ref($_[0]) eq "SCALAR";
    _eval("JSON decoding", sub {
	$tmp = $_JSON->decode(${ $_[0] });
    }, @_);
    return($class->dejsonify($tmp));
}

#+++############################################################################
#                                                                              #
#  (de)serialization                                                           #
#                                                                              #
#---############################################################################

#
# serialize (= transform into a binary string)
#

sub serialize : method {
    my($self, $tmp);

    $self = shift(@_);
    $tmp = $self->stringify_ref(@_);
    return($$tmp) unless $$tmp =~ /[\x80-\xff]/;
    _eval("UTF-8 encoding", sub {
	$tmp = Encode::encode("UTF-8", $$tmp, Encode::FB_CROAK|Encode::LEAVE_SRC);
    });
    return($tmp);
}

sub serialize_ref : method {
    my($self, $tmp);

    $self = shift(@_);
    $tmp = $self->stringify_ref(@_);
    return($tmp) unless $$tmp =~ /[\x80-\xff]/;
    _eval("UTF-8 encoding", sub {
	$tmp = Encode::encode("UTF-8", $$tmp, Encode::FB_CROAK|Encode::LEAVE_SRC);
    });
    return(\$tmp);
}

#
# deserialize (= alternate constructor using the serialized representation)
#

sub deserialize : method {
    my($class, $tmp);

    $class = shift(@_);
    validate_pos(@_, { type => SCALAR })
	unless @_ == 1 and defined($_[0]) and ref($_[0]) eq "";
    return($class->destringify($_[0])) unless $_[0] =~ /[\x80-\xff]/;
    _eval("UTF-8 decoding", sub {
	$tmp = Encode::decode("UTF-8", $_[0], Encode::FB_CROAK|Encode::LEAVE_SRC);
    }, @_);
    return($class->destringify($tmp));
}

sub deserialize_ref : method {
    my($class, $tmp);

    $class = shift(@_);
    validate_pos(@_, { type => SCALARREF })
	unless @_ == 1 and defined($_[0]) and ref($_[0]) eq "SCALAR";
    return($class->destringify_ref($_[0])) unless ${ $_[0] } =~ /[\x80-\xff]/;
    _eval("UTF-8 decoding", sub {
	$tmp = Encode::decode("UTF-8", ${ $_[0] }, Encode::FB_CROAK|Encode::LEAVE_SRC);
    }, @_);
    return($class->destringify($tmp));
}

1;

__DATA__

=head1 NAME

Messaging::Message - abstraction of a message

=head1 SYNOPSIS

  use Messaging::Message;

  # constructor + setters
  $msg = Messaging::Message->new();
  $msg->body("hello world");
  $msg->header({ subject => "test" });
  $msg->header_field("message-id", 123);

  # fancy constructor
  $msg = Messaging::Message->new(
      body => "hello world",
      header => {
          "subject"    => "test",
          "message-id" => 123,
      },
  );

  # getters
  if ($msg->body() =~ /something/) {
      ...
  }
  $id = $msg->header_field("message-id");

=head1 DESCRIPTION

This module provides an abstraction of a "message", as used in
messaging, see for instance:
L<http://en.wikipedia.org/wiki/Enterprise_messaging_system>.

A message consists of header fields (collectively called "the header
of the message") and a body.

Each header field is a key/value pair where the key and the value are
text strings. The key is unique within the header so we can use a hash
table to represent the header of the message.

The body is either a text string or a binary string. This distinction
is needed because text may need to be encoded (for instance using
UTF-8) before being stored on disk or sent across the network.

To make things clear:

=over

=item *

a I<text string> (aka I<character string>) is a sequence of Unicode
characters

=item *

a I<binary string> (aka I<byte string>) is a sequence of bytes

=back

Both the header and the body can be empty.

=head1 JSON MAPPING

In order to ease message manipulation (e.g. exchanging between
applications, maybe written in different programming languages), we
define here a standard mapping between a Messaging::Message object and
a JSON object.

A message as defined above naturally maps to a JSON object with the
following fields:

=over

=item header

the message header as a JSON object (with all values being JSON
strings)

=item body

the message body as a JSON string

=item text

a JSON boolean specifying whether the body is text string (as opposed
to binary string) or not

=item encoding

a JSON string describing how the body has been encoded (see below)

=back

All fields are optional and default to empty/false if not present.

Since JSON strings are text strings (they can contain any Unicode
character), the message header directly maps to a JSON object. There
is no need to use encoding here.

For the message body, this is more complex. A text body can be put
as-is in the JSON object but a binary body must be encoded beforehand
because JSON does not handle binary strings. Additionally, we want to
allow body compression in order to optionally save space. This is
where the encoding field comes into play.

The encoding field describes which transformations have been applied
to the message body. It is a C<+> separated list of transformations
that can be:

=over

=item base64

Base64 encoding (for binary body or compressed body)

=item utf8

UTF-8 encoding (only needed for a compressed text body)

=item zlib

Zlib compression

=back

Here is for instance the JSON object representing an empty message
(i.e. the result of Messaging::Message->new()):

  {}

Here is a more complex example, with a binary body:

  {
    "header":{"subject":"demo","destination":"/topic/test"},
    "body":"YWJj7g==",
    "encoding":"base64"
  }

You can use the jsonify() method to convert a Messaging::Message
object into a hash reference representing the equivalent JSON object.

Conversely, you can create a new Messaging::Message object from a
compatible JSON object (again, a hash reference) with the dejsonify()
method.

Using this JSON mapping of messages is very convenient because you can
easily put messages in larger JSON data structures. You can for
instance store several messages together using a JSON array of these
messages.

Here is for instance how you could construct a message containing in
its body another message along with error information:

  use JSON qw(to_json);
  # get a message from somewhere...
  $msg1 = ...;
  # jsonify it and put it into a simple structure
  $body = {
      message => $msg1->jsonify(),
      error   => "an error message",
      time    => time(),
  };
  # create a new message with this body
  $msg2 = Messaging::Message->new(body => to_json($body));
  $msg2->header_field("content-type", "message/error");
  $msg2->text(1);

A receiver of such a message can easily decode it:

  use JSON qw(from_json);
  # get a message from somewhere...
  $msg2 = ...;
  # extract the body which is a JSON object
  $body = from_json($msg2->body());
  # extract the inner message
  $msg1 = Messaging::Message->dejsonify($body->{message});

=head1 STRINGIFICATION AND SERIALIZATION

In addition to the JSON mapping described above, we also define how to
stringify and serialize a message.

A I<stringified message> is the string representing its equivalent
JSON object. A stringified message is a text string and can for
instance be used in another message. See the stringify() and
destringify() methods.

A I<serialized message> is the UTF-8 encoding of its stringified
representation. A serialized message is a binary string and can for
instance be stored in a file. See the serialize() and deserialize()
methods.

For instance, here are the steps needed in order to store a message
into a file:

=over

=item 1

transform the programming language specific abstraction of the message
into a JSON object

=item 2

transform the JSON object into its (text) string representing

=item 3

transform the JSON text string into a binary string using UTF-8
encoding

=back

"1" is called I<jsonify>, "1 + 2" is called I<stringify> and "1 + 2 +
3" is called I<serialize>.

To sum up:

        Messaging::Message object
                 |  ^
       jsonify() |  | dejsonify()
                 v  |
    JSON compatible hash reference
                 |  ^
     JSON encode |  | JSON decode
                 v  |
             text string
                 |  ^
    UTF-8 encode |  | UTF-8 decode
                 v  |
            binary string

=head1 METHODS

The following methods are available:

=over

=item new([OPTIONS])

return a new Messaging::Message object (class method)

=item dejsonify(HASHREF)

return a new Messaging::Message object from a compatible JSON object
(class method)

=item destringify(STRING)

return a new Messaging::Message object from its stringified representation
(class method)

=item deserialize(STRING)

return a new Messaging::Message object from its serialized representation
(class method)

=item jsonify([OPTIONS])

return the JSON object (a hash reference) representing the message

=item stringify([OPTIONS])

return the text string representation of the message

=item serialize([OPTIONS])

return the binary string representation of the message

=item body([STRING])

get/set the body attribute, which is a text or binary string

=item header([HASHREF])

get/set the header attribute, which is a hash reference
(note: the hash reference is used directly, without any deep copy)

=item header_field(NAME[, VALUE])

get/set the given header field, identified by its name

=item text([BOOLEAN])

get/set the text attribute, which is a boolean indicating whether the
message body is a text string or not, the default is false (so binary
body)

=item size()

get the approximate message size, which is the sum of the sizes of its
components: header key/value pairs and body, plus framing

=item copy()

return a new message which is a copy of the given one, with deep copy
of the header and body

=back

The jsonify(), stringify() and serialize() methods can be given
options. Currently, the only supported option is C<compression> and
the only supported compression is C<zlib> (when available). Here is
for instance how to serialize a message, with compression:

  $bytes = $msg->serialize(compression => "zlib");

In addition, in order to avoid string copies, the following methods
are also available:

=over

=item body_ref([STRINGREF])

=item stringify_ref([OPTIONS])

=item destringify_ref(STRINGREF)

=item serialize_ref([OPTIONS])

=item deserialize_ref(STRINGREF)

=back

They work like their counterparts but use as input or output string
references instead of strings, which can be more efficient for large
strings. Here is an example:

  # get a copy of the body, yielding to internal string copy
  $body = $msg->body();
  # get a reference to the body, with no string copies
  $body_ref = $msg->body_ref();

=head1 SEE ALSO

L<Compress::Zlib>,
L<Encode>,
L<JSON>.

=head1 AUTHOR

Lionel Cons L<http://cern.ch/lionel.cons>

Copyright CERN 2011-2012
