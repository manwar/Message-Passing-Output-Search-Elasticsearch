use strict;
use warnings;
use Test::More;
use Test::Exception;
use Search::Elasticsearch::TestServer;
use Message::Passing::Output::Search::Elasticsearch;

my $server =
    Search::Elasticsearch::TestServer->new(
    es_home => '/usr/share/elasticsearch' );

my $nodes;

# work around non-checked exec in TestServer which forks
my $pid = $$;
eval { $nodes = $server->start };
exit
    unless $pid == $$;

plan skip_all => "Can't run tests without Elasticsearch server"
    if $@;

my $out_es;

lives_ok {
    $out_es = Message::Passing::Output::Search::Elasticsearch->new(
        es_params  => { nodes => $nodes, },
        type       => 'syslog',
        index_name => 'syslog',
    );
}
'output instantiated using es_params';

my $es;
lives_ok {
    $es = Search::Elasticsearch->new( nodes => $nodes );
}
'Search::Elasticsearch instantiated';

lives_ok {
    $out_es = Message::Passing::Output::Search::Elasticsearch->new(
        es         => $es,
        type       => 'syslog',
        index_name => 'syslog',
    );
}
'output instantiated using es';

lives_ok {
    $out_es = Message::Passing::Output::Search::Elasticsearch->new(
        es_bulk    => Search::Elasticsearch::Bulk->new( es => $es ),
        type       => 'syslog',
        index_name => 'syslog',
    );
}
'output instantiated using es_bulk';

lives_ok {
    $out_es = Message::Passing::Output::Search::Elasticsearch->new(
        es             => $es,
        es_bulk_params => { max_count => 1 },
        type           => 'syslog',
        index_name     => 'syslog',
    );
}
'output instantiated using es_bulk_params';

lives_ok { $out_es->consume('text message'); } 'text message consumed';

# ensure that Elasticsearch returns the newly indexed document
$out_es->es->indices->refresh();

is $out_es->es->count()->{count}, 0, "and wasn't indexed";

lives_ok {
    $out_es->consume(
        { timestamp => 12345678, message => 'hashref message' } );
}
'hashref message consumed';

# ensure that Elasticsearch returns the newly indexed document
$out_es->es->indices->refresh();

is $out_es->es->count( index => 'syslog', type => 'syslog' )->{count}, 1,
    "and was indexed";

done_testing;
