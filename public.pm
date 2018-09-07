package public;
use Data::Dumper;

#### 公共函数
#### 2015-06-29
##
#    TimetoChar: 时间值转字符
#    ChartoTime: 字符转时间值
#    CharVerify: 字符串验证符合时间格式
#
#
#
#
#
#
##
use POSIX qw(mktime);
use Term::Complete;

our(@ISA,@EXPORT);
require Exporter;

@ISA=qw(Exporter);
@EXPORT_OK = qw(&TimetoChar, &CharVerify, &ChartoTime);

sub TimetoChar{
    my ( $tm ) = @_;
    my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime($tm);
    $year += 1900;
    $mon += 1;
    $mon = '0'.$mon if ( $mon < 10 );
    $mday = '0'.$mday if ( $mday < 10 );
    return ( $year.$mon.$mday );
}

sub CharVerify{
    my ( $char ) = @_;
    my $date;
    
    if ( $char =~ /^\d{8}$/ )
    {
        @$date = unpack("A4A2A2", $char);
    }
    elsif ( $char =~ /^\d{4}-\d{2}-\d{2}$/ )
    {
        @$date = unpack("A4xA2xA2", $char);
    }
    elsif ( $char =~ /^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}$/ )
    {
        @$date = unpack("A4xA2xA2xA2xA2xA2", $char);
    }
    elsif ( $char =~ /^\s*$/ )
    {
        return undef;
    }
    ## return a list, [yyyy, mm, dd]
    return $date;
}

sub ChartoTime{
    my ( $char ) = @_;
    my $tm_arr =  CharVerify($char);
    return mktime(0, 0, 0, $tm_arr->[2], $tm_arr->[1] - 1, $tm_arr->[0] - 1900 );
}


sub findMatch{
    my ($list, $word) = @_;
    my $match = [];

    foreach ( @$list )
    {
        if ( $_ =~ /.*$word.*/ )
        {
            push @$match, $_;
        }
    }
    return $match;
}

sub AInput{
    my ($want_list) = @_;

    while ( 1 )
    {
        my $input = Complete("->",@$want_list);    #"\n"经过Complete变为空字符
        last unless ( $input );

        $input =~ s/\s*$//g;

        if ( $input =~ /^#(\S+)\s*/ )
        {
            return $1;
        }
        my $answer = findMatch($want_list, $input);

        if ( scalar (@$answer) == 0 )
        {
            print "wrong match, input again or enter to return:\n";
            next;
        }
        elsif ( scalar(@$answer) == 1 )
        {
            return $answer->[0];
        }
        else
        {
            print "\n";
            foreach (0..20)
            {
                next unless($answer->[$_]);
                print "$answer->[$_]\n";
            }
            if ( scalar(@$answer) > 20 )
            {
                print "...\n";
                print "Still have ", scalar(@$answer) - 20, " lists matched, C&P or input more:\n";
            }
            else
            {
                print "C&P or input another:\n"
            }
            next;
        }
    }
}

1;
