package Message::Passing::Output::Search::Elasticsearch;

# ABSTRACT: index messages in Elasticsearch

use Moo;
use MooX::Types::MooseLike::Base
    qw( Str ArrayRef HashRef CodeRef is_CodeRef AnyOf ConsumerOf InstanceOf );

#use Sub::Quote qw( quote_sub );
use Search::Elasticsearch;
use Search::Elasticsearch::Bulk;

with 'Message::Passing::Role::Output';

=head1 DESCRIPTION

This output is intentionally kept simple to not add dependencies.
If you need a special format use a filter like
L<Message::Passing::Filter::ToLogstash> before sending messages to this
output.
The only two Elasticsearch specific fields are type and index name.

=cut

=head1 ATTRIBUTES

=head2 es_params

A hashref of L<Search::Elasticsearch/"CREATING A NEW INSTANCE"> parameters.

=cut

has es_params => (
    is      => 'ro',
    isa     => HashRef,
    default => sub { {} },
);

=head2 es

An object conforming to the L<Search::Elasticsearch::Role::Client> role. Can either be
passed directly or gets constructed from L</es_params>.

=cut

has es => (
    is      => 'ro',
    lazy    => 1,
    isa     => ConsumerOf ['Search::Elasticsearch::Role::Client'],
    builder => sub {
        my $self = shift;
        return Search::Elasticsearch->new( %{ $self->es_params } );
    },
);

=head2 es_bulk_params

A hashref of L<Search::Elasticsearch::Bulk/"CREATING A NEW INSTANCE"> parameters.

=cut

has es_bulk_params => (
    is      => 'ro',
    isa     => HashRef,
    default => sub { {} },
);

=head2 es_bulk

A L<Search::Elasticsearch::Bulk> instance. Can either be passed directly or gets
constructed from L</es_bulk_params> in which case 'es' is set to L</es>.

=cut

has es_bulk => (
    is      => 'ro',
    lazy    => 1,
    isa     => InstanceOf ['Search::Elasticsearch::Bulk'],
    builder => sub {
        my $self = shift;
        return Search::Elasticsearch::Bulk->new( %{ $self->es_bulk_params },
            es => $self->es );
    },
);

=head2 type

Can be either set to a fixed string or a coderef that's called for every
message to return the type depending on the contents of the message.

=cut

has type => (
    is       => 'ro',
    required => 1,
    isa      => AnyOf [ Str, CodeRef ],
);

=head2 index_name

Can be either set to a fixed string or a coderef that's called for every
message to return the index name depending on the contents of the message.

=cut

has index_name => (
    is       => 'ro',
    required => 1,
    isa      => AnyOf [ Str, CodeRef ],
);

=head1 METHODS

=head2 consume ($msg)
 
Consumes a message, queuing it for consumption by Elasticsearch.
Assumes that the message is a hashref, skips silently in case it isn't.

=cut

sub consume {
    my ( $self, $data ) = @_;
    return
        unless defined $data && ref $data eq 'HASH';

    #if ( my $epochtime = delete $data->{epochtime} ) {
    #$date = DateTime->from_epoch(epoch => $epochtime);
    #}
    #$date ||= DateTime->from_epoch(epoch => time());

    my $type =
        is_CodeRef( $self->type )
        ? $self->type->($data)
        : $self->type;
    my $index_name =
        is_CodeRef( $self->index_name )
        ? $self->index_name->($data)
        : $self->index_name;

    #$self->_indexes->{$index_name} = 1;
    #    my $to_queue = {
    #        '@timestamp'   => to_ISO8601DateTimeStr($date),
    #        '@tags'        => [],
    #        '@type'        => $type,
    #        '@source_host' => delete( $data->{hostname} ) || 'none',
    #        '@message'     => exists( $data->{message} )
    #        ? delete( $data->{message} )
    #        : encode_json($data),
    #        '@fields' => $data,
    #        exists( $data->{uuid} ) ? ( id => delete( $data->{uuid} ) ) : (),
    #    };
    $self->es_bulk->create(
        {   index  => $index_name,
            type   => $type,
            source => $data,
        }
    );
}

1;

=head1 SEE ALSO
 
=over
 
=item L<Message::Passing>

=back

=cut
