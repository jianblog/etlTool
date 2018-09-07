package EtlSource;
#require Exporter;
#@ISA = qw(Exporter);

  ##########
  # Date: 2015-02-10
  # Desc: new EtlSource
  #
  ##########

use Storable;
use DBI;
use DBD::DB2;
use IO::File;
use FindBin qw($Bin);
use Data::Dumper;

use constant MODEL_DIR => $Bin."/models";
use constant ETLCFG => "/gpfsedwprog/etlplus/script/conf/etlplus.cfg";

$VERSION = 2.10;

{
    # closure: 用于保存私有类变量: datasource
    my $_datasource = undef;
    my $data_count = 0;
    sub _dbConnect{
        unless ( $_datasource )
        {
            $data_count = 1;
            ## 初始化数据源
            my ($db,$usr,$pwd,$auto_commit) = _getRepository(ETLCFG);
            $auto_commit = 1 if(not defined($auto_commit));

            $_datasource = DBI->connect("dbi:DB2:$db", $usr, $pwd, { AutoCommit => $auto_commit, PrintError => 1, RaiseError => 0 } ) or die "Cannot connect $db : $DBI::errstr";
        }
        return $_datasource;
    }

    sub _getRepository{
        my ($conf_file) = @_;
        unless ( $conf_file )
        {
            $conf_file = "/file_share/ETLPLUS/script/conf/etlplus.cfg";
            #$conf_file = $ENV{"ETLPLUS_HOME"}."/script/conf/etlplus.cfg";
        }
        my ($type,$db,$usr,$pwd);
        open(CFG,"$conf_file") or die "未找到文件etlplus.cfg, 请修改EtlSource.pm, 将 ETLCFG 改为本地 etlplus.cfg全路径";
        while(<CFG>){
            my $line=$_;
            $line=~s/ |\n//g;
            ($type,$db)  = split(/=/,$line) if($line =~/db_name/);
            ($type,$usr) = split(/=/,$line) if($line =~/db_user/);
            ($type,$pwd) = split(/=/,$line) if($line =~/db_pwd/);
        }
        close(CFG);
        return($db,$usr,$pwd);
    }
    
    sub _getCount{
        print "--- $data_count ----\n";
    }
# closure end
}



sub new{
    my ($class) = @_;
    my $self = {};

    $self->{'PARA'} = {};
    
    $self->{'FLOW'} = {};
    $self->{'JOB'} = {};
    $self->{'TRIGGER'} = {};
    $self->{'DEPENDENCE'} = {};
    $self->{'SCHEDULE'} = {};

    bless $self, $class;
    return $self;
}

# SetAbbr, GetRepository, GetConnection
sub etlSys{
    my ($class, $etlsys) = @_;

    $etlsys? $class->{'PARA'}->{'ETLSYS'} = $etlsys
           : $class->{'PARA'}->{'ETLSYS'};
}


sub initModel{
    my ($class, $etl_sys_nm) = @_;

    die "NO system server name defined!\n" unless ( $etl_sys_nm );

    my $sql_sys =  "select system_id, system_nm, system_abbr_nm from system where system_abbr_nm ='"
                   ."$etl_sys_nm"
                   ."' with ur";
    my $sys_key = ['SYSTEM_NM', 'SYSTEM_ABBR_NM'];

    my $sys_chk = $class->queryDB( $sql_sys, $sys_key);
    unless ( $sys_chk )
    {
        my $dbh = _dbConnect();
        $dbh->disconnect();
        die "$etl_sys_nm has not defined in Etlplus Server.\n";
    }    
    $class->etlSys($etl_sys_nm);

    ## 基础数据中，每一种属性有特点的sql以及对应sql的字段名称
    my $query_sql = {};
    my $query_key = {};

    #$query_sql->{'FLOW'} = "select flow.flow_id, flow.flow_nm, flow.enable, flow.disableddate, flow.flow_date, add.job_id"
    #               ." from job_flow flow, system sys, flow_addition add where "
    #               ." flow.system_id=sys.system_id and flow.flow_id=add.flow_id and system_abbr_nm='"
    #               ."$etl_sys_nm"
    #               ."' with ur";
   #- $query_sql->{'FLOW'} = "select flow.flow_id, flow.flow_nm, flow.enable, flow.disableddate, flow.flow_date, job.name"
   #-            ." from job_flow flow, system sys, flow_addition add, etl_job job where "
   #-            ." flow.system_id=sys.system_id and flow.flow_id=add.flow_id and add.job_id=job.id and system_abbr_nm='"

    $query_sql->{'FLOW'} = "select flow.flow_id, flow.flow_nm, flow.enable, flow.disableddate, flow.flow_date, job.name"
            ." from job_flow flow left join flow_addition add on flow.flow_id=add.flow_id  "
            ." left join etl_job job on add.job_id=job.id  "
            ." inner join system sys  on flow.system_id=sys.system_id "
            ." and system_abbr_nm='"
            ."$etl_sys_nm"
            ."' with ur";

    $query_key->{'FLOW'} = ['01_FLOW_NM', '02_ENABLE', '03_DISABLEDDATE', '04_FLOW_DATE', '05_UPDT_JOB'];
    #$query_key->{'FLOW'} = ['01_FLOW_NM', '02_ENABLE', '03_DISABLEDDATE', '04_FLOW_DATE', '05_UPDT_JOB_ID'];

    $query_sql->{'JOB'} = "select job.id, job.name, etlsys.name, flow.flow_nm," 
                   ." job.enable, add.job_tp, add.period_tp, add.prog_nm,  src.file_nm "
                   ." from etl_job job inner join etl_sys etlsys on job.sys_id=etlsys.id "
                   ." inner join job_addition add on add.id=job.id  inner join system sys on sys.system_id=add.system_id"
                   ." left join job_flow flow on flow.flow_id=add.flow_id"
                   ." left join  src_file src on job.id=src.job_id"
                   ." where sys.system_abbr_nm ='"
                   ."$etl_sys_nm"
                   ."' with ur";
    $query_key->{'JOB'} = ['01_NAME', '02_SUBSYS', '03_FLOW_NM', '04_ENABLE', '05_JOB_TP', '06_PERIOD_TP', '07_PROG_NM', '08_FILE_NM'];
                    
    $query_sql->{'TRIGGER'} = "select stream.id, stream.job_id, stream.stream_id from etl_job_stream stream where stream.job_id in "
                   ." (select job.id from etl_job job, job_addition add,system sys where sys.system_abbr_nm ='"
                   ."$etl_sys_nm"
                   ."' and sys.system_id = add.system_id and add.id=job.id) with ur"; 
    $query_key->{'TRIGGER'} = ['JOB_ID', 'STREAM_ID'];
                   
    $query_sql->{'DEPENDENCE'} = "select dep.id, dep.job_id, dep.dependence_id from etl_job_dependence dep "
                   ." inner join etl_job job on job.id = dep.job_id "
                   ." where dep.job_id in (select job.id from etl_job job, job_addition add, "
                   ." system sys where sys.system_abbr_nm ='"
                   ."$etl_sys_nm"
                   ."' and sys.system_id = add.system_id and add.id=job.id)  with ur ";
    $query_key->{'DEPENDENCE'} = ['JOB_ID', 'DEPENDENCE_ID'];
                  
    $query_sql->{'SCHEDULE'} = "select cron.id, cron.name, job.id, cron.sys_name, cron.schedule_type, "
                   ." cron.start_time, cron.begin_date, cron.end_date from etl_schedule_job cron, etl_job job where "
                   ." sys_name in (select name from etl_sys where id in "
                   ." (select distinct sys_id from etl_job job,system sys,job_addition add "
                   ." where sys.system_abbr_nm='"
                   ."$etl_sys_nm"
                   ."' and sys.system_id=add.system_id and job.id=add.id )) and cron.job_name=job.name with ur";
    $query_key->{'SCHEDULE'} = ['01_NAME', '02_JOB_ID', '03_SUBSYS', '04_SCHEDULE_TYPE', '05_START_TIME', '06_BEGIN_DATE', '07_END_DATE'];
                   

    ## 将基础数据返回的结果作为类属性
    foreach ( keys %$query_sql )
    {
        $class->{$_} = $class->queryDB($query_sql->{$_}, $query_key->{$_});
    }    
    
    # save EtlSource
    $class->storeModel();
}

##  数据库查询，根据是否含指定key_arr，返回hash或array
sub queryDB{
    my ( $class, $sql, $key_arr ) = @_;

    my $dbh = _dbConnect();
    my $sth = $dbh->prepare($sql) or $dbh->disconnect() and die("Cannot prepare SQL: $sql, ".$dbh->errstr());
    $sth->execute() or $dbh->disconnect() and die("Cannot execute SQL: $sql . ".$dbh->errstr());

    if ( $key_arr )
    {
        my $snap = {};
    
        while ( my @rows = $sth->fetchrow_array() )
        {
            my $key = shift @rows;
            my $ref = {};
        
            ## 配对两个array 生成hash
            @$ref{ @$key_arr } = @rows;
            $snap->{$key} = $ref;
        
        }
        return $snap;
    }
    else
    {
        my $snap = [];
        
        while ( my @rows = $sth->fetchrow_array() )
        {
            push @$snap, \@rows;
        }
        return $snap;
    }
}

##  数据库查询, 创建hash 
sub __queryDB{       
    my ( $class, $sql, $key_arr ) = @_;

    my $dbh = _dbConnect();
    my $snap = {};
    my $sth = $dbh->prepare($sql) or $dbh->disconnect() and die("Cannot prepare SQL: $sql, ".$dbh->errstr());
    $sth->execute() or $dbh->disconnect() and die("Cannot execute SQL: $sql . ".$dbh->errstr());
                   
    while ( my @rows = $sth->fetchrow_array() )
    {
        my $key = shift @rows;
        my $ref = {}; 
                   
        ## 配对两个array 生成hash
        @$ref{ @$key_arr } = @rows;      
        $snap->{$key} = $ref;
    
    }              
    return $snap;
}

sub storeModel{
    my $class = shift;
    my $sys_nm = $class->etlSys();
    
    my $flag;
    foreach (keys %{ $class } )
    {
        my $filename = $sys_nm."_".$_.".dbm";        
        my $dir = MODEL_DIR."/".$sys_nm;
        mkdir MODEL_DIR;
        mkdir $dir or die "Cannot create directory:$dir.\n" unless ( -d $dir );
        my $try = eval{
            store $class->{$_}, $dir."/".$filename;
            $flag = $flag."$_:$filename\n"; 
        };
        if ( ! defined ($try) )
        {
            die " Cannot store Etlmodel to file: $filename.\n";
        }
        
        open(my $fh, ">", $dir."/".$sys_nm.".id");
        print $fh $flag;
        $fh->close();
    }
}

sub loadModel{
    my ($class, $id_path, $etl_sys_nm) = @_;

    $class->etlSys($etl_sys_nm);
    my $id_file = $id_path."/".$etl_sys_nm.".id";
    open(my $fh, "<", $id_file);
    foreach ( <$fh> )
    {
        my ($para, $file) = split /:/, $_;
        my $abs_file = $id_path."/".$file;

        $class->{$para} = retrieve($abs_file);
    }
        if (exists $class->{'FLOW'})
    {
        foreach ( keys %{ $class->{'FLOW'} } )
        {
            $class->{'REVERSE_FLOW'}->{ $class->{'FLOW'}->{$_}->{'01_FLOW_NM'} } = $_;
        }
    }
    if ( exists $class->{'JOB'} )
    {
        foreach ( keys %{ $class->{'JOB'} } )
        {
            $class->{'REVERSE_JOB'}->{ $class->{'JOB'}->{$_}->{'01_NAME'} } = $_;
        }
    }
}

          
1; 
