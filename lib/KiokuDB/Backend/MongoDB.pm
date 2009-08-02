package KiokuDB::Backend::MongoDB;
use Moose;

use namespace::clean -except => 'meta';
our $VERSION = '0.01';

with qw(
         KiokuDB::Backend
         KiokuDB::Backend::Serialize::JSPON
         KiokuDB::Backend::Role::UnicodeSafe
         KiokuDB::Backend::Role::Clear
         KiokuDB::Backend::Role::Scan
         
);

# TODO: 
#   KiokuDB::Backend::Role::Query::Simple
#   http://search.cpan.org/~nuffin/KiokuDB-0.31/lib/KiokuDB/Backend/Role/Query/Simple.pm

use Carp qw(croak);

has collection => (
    isa => 'MongoDB::Collection',
    is  => 'ro',
);

has '+id_field'    => ( default => "_id" );
has '+class_field' => ( default => "class" );
has '+class_meta_field' => ( default => "class_meta" );

sub new_from_dsn_params {
    my ($self, %args) = @_;

    my $mongodb;

    unless ($args{collection}) {
        $args{host} ||= 'localhost';
        $args{port} ||= 27017;
        $args{database_name}   or croak "database parameter required";
        $args{collection_name} or croak "collection parameter required";
        $mongodb =
          MongoDB::Connection->new(host => $args{host}, port => $args{port});
    }


    my $collection = $args{collection} ||
      $mongodb->get_database($args{database})
      ->get_collection($args{collection});

    $self->new(%args, collection => $collection);
}

sub clear {
    my $self = shift;
    $self->collection->drop;
}

sub all_entries {
    my $self = shift;
    bulk($self->collection->find());
}

sub insert {
    my ($self, @entries) = @_;

    my $coll = $self->collection;

    use Data::Dump qw(pp);

    for my $entry (@entries) {
        my $collapsed = $self->collapse_jspon($entry); 
        if ($entry->prev) {
            $coll->update({ _id => $collapsed->{_id} }, $collapsed);
        }
        else {
            $coll->insert($collapsed);
            my $err = $coll->_database->run_command({getlasterror => 1});
            die $err->{err} if $err->{err};
        }
    }
    return;
}

sub get {
    my ($self, @ids) = @_;
    my $coll = $self->collection;
    map { $self->deserialize($_) }
      map {
        $coll->find_one({_id => $_})
          or die {missing => 1};

      } @ids;
}

sub delete {
    my ( $self, @ids_or_entries ) = @_;
    my $coll = $self->collection;
    my @ids = map { $_->{_id} } grep { ref } @ids_or_entries;
    push @ids, grep { not ref } @ids_or_entries;
    for my $id (@ids) {
        $coll->remove({ _id => $id });
    }
    
}

sub deserialize {
    my ( $self, $doc ) = @_;
    my %doc = %{ $doc };
    return $self->expand_jspon(\%doc);
}

sub exists {
    my ($self, @ids) = @_;
    my $coll = $self->collection;
    map { $coll->find_one({ _id => $_ }) } @ids;
}



__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

KiokuDB::Backend::MongoDB - MongoDB backend for KiokuDB

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';


=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use KiokuDB::Backend::MongoDB;

    my $foo = KiokuDB::Backend::MongoDB->new();
    ...

=head1 FUNCTIONS

=head2 function1

=cut

sub function1 {
}

=head2 function2

=cut

sub function2 {
}

=head1 AUTHOR

Ask Bjørn Hansen, C<< <ask at develooper.com> >>

=head1 BUGS

Please report any bugs or feature requests to
C<bug-kiokudb-backend-mongodb at rt.cpan.org>, or through the web
interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=KiokuDB-Backend-MongoDB>.
I will be notified, and then you'll automatically be notified of
progress on your bug as I make changes.


=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc KiokuDB::Backend::MongoDB

You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=KiokuDB-Backend-MongoDB>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/KiokuDB-Backend-MongoDB>

=item * Search CPAN

L<http://search.cpan.org/dist/KiokuDB-Backend-MongoDB/>

=back


=head1 ACKNOWLEDGEMENTS

Yuval Kogman (KiokuDB::Backend::CouchDB) and Florian Ragwitz (MongoDB).

=head1 COPYRIGHT & LICENSE

Copyright 2009 Ask Bjørn Hansen, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut
