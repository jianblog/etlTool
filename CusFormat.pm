package CusFormat;

$VERSION = 1.02;

## desc: 装饰器模式，设计一个包含多种输出外观的格式控制
##  起始字符分别为不同形式的格式
sub new{
    my ( $class ) = @_;
    my $self = {};
    $self->{'COM'} = undef;
     
    bless $self, $class;
}
sub decorate{
    my ( $class, $obj ) = @_;
    $class->{'COM'} = $obj;
}
sub combing{
    my ( $class, $str_ref, $final ) = @_;

    my $rt = $class->{'COM'}->combing($str_ref, $final);

    return $rt;
}

package upcorFormat;
use base CusFormat;
sub combing{
    my ( $class, $str_ref, $final ) = @_;

    push @$final, "┌ ";
    my $rt = $class->{'COM'}->combing($str_ref, $final);

    return $rt;
}

package downcorFormat;
use base CusFormat;
sub combing{
    my ( $class, $str_ref, $final ) = @_;

    push @$final, "└ ";
    my $rt = $class->{'COM'}->combing($str_ref, $final);

    return $rt;
}

package rowFormat;
use base CusFormat;
sub combing{
    my ( $class, $str_ref, $final ) = @_;

    push @$final, "│ ";
    my $rt = $class->{'COM'}->combing($str_ref, $final);

    return $rt;
}

package spaceFormat;
use base CusFormat;
sub combing{
    my ( $class, $str_ref, $final ) = @_;

    push @$final, "    ";
    my $rt = $class->{'COM'}->combing($str_ref, $final);

    return $rt;
}

1;
