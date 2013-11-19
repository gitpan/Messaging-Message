#+##############################################################################
#                                                                              #
# File: Messaging/Message/Queue/NULL.pm                                        #
#                                                                              #
# Description: abstraction of a Directory::Queue::Null message queue           #
#                                                                              #
#-##############################################################################

#
# module definition
#

package Messaging::Message::Queue::NULL;
use strict;
use warnings;
our $VERSION  = "1.5";
our $REVISION = sprintf("%d.%02d", q$Revision: 1.5 $ =~ /(\d+)\.(\d+)/);

#
# inheritance
#

our @ISA = qw(Messaging::Message::Queue Directory::Queue::Null);

#
# used modules
#

use Messaging::Message qw(_require);
use No::Worries::Die qw(dief);
use Params::Validate qw(validate_with validate_pos :types);

#
# constructor
#

sub new : method {
    my($class, %option, $self);

    _require("Directory::Queue::Null");
    $class = shift(@_);
    %option = validate_with(
        params      => \@_,
        spec        => {},
        allow_extra => 0,
    );
    $self = Directory::Queue::Null->new(%option);
    bless($self, $class);
    return($self);
}

#
# add a message object to the queue
#

sub add_message : method {
    my($self, $msg);

    $self = shift(@_);
    validate_pos(@_, { isa => "Messaging::Message" });
    $msg = shift(@_);
    return($self->add($msg));
}

#
# get a message object from the queue
#

sub get_message : method {
    my($self, $elt);

    $self = shift(@_);
    validate_pos(@_, { type => SCALAR });
    $elt = shift(@_);
    # the next line should trigger a fatal error as the queue is always empty
    $self->get($elt);
    dief("ooops");
}

1;

__DATA__

=head1 NAME

Messaging::Message::Queue::NULL - abstraction of a Directory::Queue::Null message queue

=head1 SYNOPSIS

  use Messaging::Message;
  use Messaging::Message::Queue::NULL;

  # create a message queue
  $mq = Messaging::Message::Queue::NULL->new();

  # add a message to the queue
  $msg = Messaging::Message->new(body => "hello world");
  $mq->add_message($msg);

=head1 DESCRIPTION

This module provides an abstraction of a message queue. It derives
from the L<Directory::Queue::Null> module that provides a generic
"black hole" queue: added messages will disappear immediately so the
queue will therefore always appear empty.

=head1 METHODS

In addition to the methods inherited from L<Directory::Queue::Null>,
the following methods are available:

=over

=item new(OPTIONS)

return a new Messaging::Message::Queue::NULL object (class method)

=item add_message(MESSAGE)

add the given message (a Messaging::Message object) to the queue,
this does nothing

=item get_message(ELEMENT)

get the message from the given element, this generates an error

=back

=head1 SEE ALSO

L<Directory::Queue::Null>,
L<Messaging::Message>,
L<Messaging::Message::Queue>.

=head1 AUTHOR

Lionel Cons L<http://cern.ch/lionel.cons>

Copyright (C) CERN 2011-2013
