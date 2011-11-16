#+##############################################################################
#                                                                              #
# File: Messaging/Message/Queue/DQ.pm                                          #
#                                                                              #
# Description: abstraction of a Directory::Queue message queue                 #
#                                                                              #
#-##############################################################################

#
# module definition
#

package Messaging::Message::Queue::DQ;
use strict;
use warnings;
our $VERSION  = "0.5";
our $REVISION = sprintf("%d.%02d", q$Revision: 1.3 $ =~ /(\d+)\.(\d+)/);

#
# inheritance
#

our @ISA = qw(Directory::Queue);

#
# used modules
#

use Messaging::Message qw(_fatal);
use Params::Validate qw(validate_with validate_pos :types);

#
# optional modules
#

eval { require Directory::Queue };

#
# constructor
#

sub new : method {
    my($class, %option, $self);

    _fatal("missing module: Directory::Queue")
	unless $Directory::Queue::VERSION;
    $class = shift(@_);
    %option = validate_with(
        params      => \@_,
        spec        => {},
	allow_extra => 1,
    );
    $option{schema} = {
	header => "table",
	binary => "binary?",
	text   => "string?",
    };
    $self = Directory::Queue->new(%option);
    bless($self, $class);
    return($self);
}

#
# add a message object to the queue
#

sub add_message : method {
    my($self, $msg, %data);

    $self = shift(@_);
    validate_pos(@_, { isa => "Messaging::Message" });
    $msg = shift(@_);
    $data{header} = $msg->header();
    $data{$msg->text() ? "text" : "binary"} = $msg->body();
    return($self->add(\%data));
}

#
# get a message object from the queue
#

sub get_message : method {
    my($self, $elt, $data, $msg);

    $self = shift(@_);
    validate_pos(@_, { type => SCALAR });
    $elt = shift(@_);
    $data = $self->get($elt);
    $msg = Messaging::Message->new();
    $msg->header($data->{header}) if keys(%{ $data->{header} });
    if (exists($data->{text})) {
        $msg->body_ref(\$data->{text});
        $msg->text(1);
    } elsif (exists($data->{binary})) {
        $msg->body_ref(\$data->{binary});
        $msg->text(0);
    }
    return($msg);
}

1;

__DATA__

=head1 NAME

Messaging::Message::Queue::DQ - abstraction of a Directory::Queue message queue

=head1 SYNOPSIS

  use Messaging::Message;
  use Messaging::Message::Queue::DQ;

  # create a message queue
  $mq = Messaging::Message::Queue::DQ->new(path => "/some/where");

  # add a message to the queue
  $msg = Messaging::Message->new(body => "hello world");
  printf("msg added as %s\n", $mq->add_message($msg));

  # browse the queue
  for ($name = $mq->first(); $name; $name = $mq->next()) {
      next unless $mq->lock($name);
      $msg = $mq->get_message($name);
      # one could use $mq->unlock($name) to only browse the queue...
      $mq->remove($name);
  }

=head1 DESCRIPTION

This module provides an abstraction of a message queue. It derives
from the L<Directory::Queue> module that provides a generic
directory-based queue.

It uses the following Directory::Queue schema to store a message:

  $schema = {
      header => "table",
      binary => "binary?",
      text   => "string?",
  };

The message header is therefore stored as a table and the message body
is stored either as a text or binary string.

=head1 METHODS

In addition to the methods inherited from L<Directory::Queue>, the
following methods are available:

=over

=item new(OPTIONS)

return a new Messaging::Message::Queue::DQ object (class method)

=item add_message(MESSAGE)

add the given message (a Messaging::Message object) to the queue and
return the corresponding element name

=item get_message(ELEMENT)

get the message from the given element (which must be locked) and
return a Messaging::Message object

=back

=head1 SEE ALSO

L<Directory::Queue>,
L<Messaging::Message>,
L<Messaging::Message::Queue>.

=head1 AUTHOR

Lionel Cons L<http://cern.ch/lionel.cons>

Copyright CERN 2011
