use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'KiokuDB::Backend::MongoDB',
    AUTHOR              => 'Ask Bjørn Hansen <ask@develooper.com>',
    VERSION_FROM        => 'lib/KiokuDB/Backend/MongoDB.pm',
    ABSTRACT_FROM       => 'lib/KiokuDB/Backend/MongoDB.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
        'Moose' => 0,

        'KiokuDB' => '0.31',
        'MongoDB' => '0.02',

        'Data::Stream::Bulk' => '0.07',

        'Scope::Guard'  => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'KiokuDB-Backend-MongoDB-*' },
);
