#!perl

use strict;
use warnings;
use charnames qw(:full);
use File::Temp qw(tempdir);
use Messaging::Message;
use Messaging::Message::Queue;
use Test::More tests => 23;

our($tmpdir, $binstr, $unistr);

sub test_m ($$) {
    my($mq, $msg1) = @_;
    my($elt, $msg2);

    $elt = $mq->add_message($msg1);
    $mq->lock($elt);
    $msg2 = $mq->get_message($elt);
    $mq->unlock($elt);
    is_deeply($msg1, $msg2, "add+get");
}

sub test_q ($) {
    my($mq) = @_;
    my($elt);

    ok($mq->isa("Messaging::Message::Queue"), "$mq isa(Messaging::Message::Queue)");
    ok($mq->isa("Messaging::Message::Queue"), "$mq isa(Directory::Queue)");
    test_m($mq, Messaging::Message->new());
    test_m($mq, Messaging::Message->new(body => $binstr, text => 0));
    test_m($mq, Messaging::Message->new(body_ref => \$unistr, header => { $unistr => $unistr }, text => 1));
    is($mq->count(), 3, "count (1)");
    for ($elt = $mq->first(); $elt; $elt = $mq->next()) {
	ok($mq->lock($elt), "lock $elt");
	$mq->remove($elt);
    }
    is($mq->count(), 0, "count (2)");
}

sub test_null ($) {
    my($mq) = @_;
    my($msg);

    ok($mq->isa("Messaging::Message::Queue"), "$mq isa(Messaging::Message::Queue)");
    ok($mq->isa("Messaging::Message::Queue"), "$mq isa(Directory::Queue)");
    $msg = Messaging::Message->new();
    $mq->add_message($msg);
    is($mq->count(), 0, "count");
}

$tmpdir = tempdir(CLEANUP => 1);
$binstr = join("", map(chr($_ ^ 123), 0 .. 255));
$unistr = "[D�j� Vu] sigma=\N{GREEK SMALL LETTER SIGMA} \N{EM DASH} smiley=\x{263a}";

SKIP : {
    eval { require Directory::Queue::Normal };
    skip("Directory::Queue::Normal is not installed", 10) if $@;
    test_q(Messaging::Message::Queue->new(type => "DQN",  path => "$tmpdir/1"));
}

SKIP : {
    eval { require Directory::Queue::Simple };
    skip("Directory::Queue::Simple is not installed", 10) if $@;
    test_q(Messaging::Message::Queue->new(type => "DQS",  path => "$tmpdir/2"));
}

SKIP : {
    eval { require Directory::Queue::Null };
    skip("Directory::Queue::Null is not installed", 3) if $@;
    test_null(Messaging::Message::Queue->new(type => "NULL"));
}
