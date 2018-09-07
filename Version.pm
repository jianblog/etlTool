package Version;
use Net::FTP;
use FindBin qw($Bin);
use Storable;
use Data::Dumper;

############################################################
#    package: Version.pm
#    date: 2014-12-24
#    desc: sync scripts version from ftpServer.
#    how: 1. add $VERSION definition in all scripts and package used.
#         2. like  next scripts in main program
#
#            $VERSION = 1.40;
#            use constant CODEHUB => "/glxt/Release/Dev/TEST/";
#
#            my $modules = ['main', 'Ma', 'Na', 'Version',];   # your 'main' and other package name
#            my $version = Version->new();
#            foreach ( @$modules )
#            {
#                $version->addModule($_, $_->VERSION);
#            }
#            $version->syncFtp(CODEHUB);
#
#update: 20141225
#        3. maintian an release.ver on CODE_FTP
#           write an package list txt like:
#             main    TEST    1.40    20141224    a.plx
#             Ma      TEST    1.2     20141223    Ma.pm
#             Na      NA      
#             Version TEST    2.04    20141222    Version.pm
#
#            comment: package name,    proejct,   versionNum,    subdirectory,    filename
#                    if the package used not belong to current project, then  it will goto the 'NA' project directory,
#                    and goto this package directory,get  release.ver to find update package
#update: 20141225
#           some problems happens at different OS between linux and aix, the binary release.ver cant load properly,
#           change to asc. 
#           <then, run makever.plx, it will create an latest version file: release.ver>
#
##########################################################

use constant CODE_HUB => "/glxt/Release/Dev/";

use constant CODE_FTP => "21.244.88.129";
use constant CODE_USER=> "glxtftp";
use constant CODE_PASS=> "glbzuser";

$VERSION = 1.3;
sub sync;
sub ftpAction;

sub new{
    my ($class) = @_;
    my $self = {};
    $self->{'MODULE'} = { };

    bless $self, $class;
    return $self;
}
sub addModule{
    my ($class, $module_name, $module_ver) = @_;
    $class->{'MODULE'}->{$module_name} = $module_ver;
}
sub showVersion{
    my ($class) = @_;
    
    foreach ( keys %{$class->{'MODULE'}} )
    {
        print "$_==========>$class->{'MODULE'}->{$_}\n";
    }
}

sub syncVer
{
    my ($class, $project) = @_;
    #$count++;
    #exit if ($count > 10);
    my $project_dir = CODE_HUB.$project;
    my $new_ver = {};
    
    my $vertxt = "release.txt";
    my $conn = ftpGet($project_dir, $vertxt, $Bin."/", $vertxt);
    return unless ( $conn );
    
    my $upt_ver = {};
    
    open (FH, '<', $vertxt);
    while(<FH>)
    {
        chomp;
        s/^\s*$//g;
        s/^\s*//g;
        s/\s*$//g;
        my $string = [];
        @$string = split /\s+/, $_;
        my $name = shift @$string;
        $upt_ver->{$name} = $string;
    }
    close(FH);
    unlink($Bin."/".$vertxt);

    my $update = [];
    foreach my $pak (keys %{ $class->{'MODULE'} })
    {
        next unless (exists $upt_ver->{$pak});

        if ( scalar( @{$upt_ver->{$pak}} ) < 4 )    
        {
        # the package locate in other code directory
            $class->syncVer( $upt_ver->{$pak}->[0] );
            next;
        }
        
        if ( $class->{'MODULE'}->{$pak} >= $upt_ver->{$pak}->[1] )
        {
            next;
        }
        else
        {
            my $pack_dir = $project_dir."/".$upt_ver->{$pak}->[2];
            my $rt = ftpGet($pack_dir, $upt_ver->{$pak}->[3], $Bin, $upt_ver->{$pak}->[3]);
            return undef unless ( $rt );
            push @$update, $upt_ver->{$pak}->[3];
        }   
        
    }
    if ( scalar( @$update ) > 0 )
    {
        foreach ( @$update )
        {
            print "package: $_ updated...\n";
        }
        print "Update finished, please run again.\n";
        exit;
    }
}

sub ftpGet
{
    my ($re_dir, $re_file, $local, $l_file) = @_;
    my $host = CODE_FTP;
    my $user = CODE_USER;
    my $pass = CODE_PASS;

    $local .= "/" if ( $local =~ /\w+$/ );
    
    my $ftp = Net::FTP->new($host) or die "cannot connect $host\n";
    $ftp->login($user, $pass) or warn $ftp->message;
    $ftp->cwd($re_dir) or warn $ftp->message;
    $ftp->binary();
    $ftp->get($re_file, $local.$l_file) or warn $ftp->message;
    $ftp->quit;
}
1;
