package Etm;


use Storable;
use FindBin qw($Bin);
use lib "$Bin";
use POSIX qw(mktime);
use Data::Dumper;

use base EtlSource;
use CusFormat;
use public;

$VERSION = 2.101;

sub new{
    my ($class) = @_;
    
    my $self = $class->SUPER::new();
    
    #$self->{'PARA'} = {};
    $self->{'PARA'}->{'SNAPMIN'} = 30;     #快照最小抓取间隔
    $self->{'PARA'}->{'LASTUPTM'} = '';        #上次快照更新时间

    #$self->{'JOBSNAP'} = {};

    return $self;
}

sub getBaseInfo{
    my ( $class, $base, $id, $item ) = @_;
    if ( $base )
    {
        if ( $id )
        {
            if ( $item )
            {
                return $class->{$base}->{$id}->{$item} if ( exists $class->{$base}->{$id}->{$item} );
                return undef;
            }

            return $class->{$base}->{$id} if ( exists $class->{$base}->{$id} );
            return undef;   # when $id is invalid
        }
        return $class->{$base} if ( exists $class->{$base} );
        return undef;
    }
    return undef;
} 

sub getDepUp{
    my ($class, $job_id) = @_;
    my $dep_up = [];
    foreach ( keys %{$class->{'DEPENDENCE'}} )
    {
        if ( $class->{'DEPENDENCE'}->{$_}->{'JOB_ID'} eq $job_id )
        {
            push @$dep_up, $class->{'DEPENDENCE'}->{$_}->{'DEPENDENCE_ID'};
        }
    }
    return undef if ( scalar(@$dep_up) == 0 );

    return $dep_up;
}

sub getDepDown{
    my ($class, $job_id) = @_;
    my $dep_down = [];

    foreach ( keys %{$class->{'DEPENDENCE'}} )
    {
        if ( $class->{'DEPENDENCE'}->{$_}->{'DEPENDENCE_ID'} eq $job_id )
        {
            push @$dep_down, $class->{'DEPENDENCE'}->{$_}->{'JOB_ID'};
        }
    }
    return undef if ( scalar(@$dep_down) == 0 );

    return $dep_down;
}
sub getTrigUp{
    my ($class, $job_id) = @_;
    my $trig_up = [];

    foreach ( keys %{$class->{'TRIGGER'}} )
    {
        if ( $class->{'TRIGGER'}->{$_}->{'STREAM_ID'} eq $job_id )
        {
            push @$trig_up, $class->{'TRIGGER'}->{$_}->{'JOB_ID'};
        }
    }
    return undef if ( scalar(@$trig_up) == 0 );

    return $trig_up;
}
sub getTrigDown{
    my ($class, $job_id) = @_;
    my $trig_list = [];
    foreach ( keys %{$class->{'TRIGGER'}} )
    {
        if ( $class->{'TRIGGER'}->{$_}->{'JOB_ID'} eq $job_id )
        {
            push @$trig_list, $class->{'TRIGGER'}->{$_}->{'STREAM_ID'};
        }
    }
    return undef if (scalar(@$trig_list) == 0 );

    return $trig_list;
}

sub getSchedule{
    my ($class, $job_id) = @_;
    my $schedule = [];
    foreach ( keys %{$class->{'SCHEDULE'}} )
    {
        if ( $class->{'SCHEDULE'}->{$_}->{'JOB_ID'} eq $job_id )
        {
            push @$schedule, $class->{'SCHEDULE'}->{$_};
        }
    }
    return undef if ( scalar(@$schedule) == 0 );
    return $schedule;
}

sub getFlowList{
    my ($class) = @_;
    return [keys %{$class->{'FLOW'}}];
}

sub FlowNameList{
    my ($class) = @_;
    return [keys %{$class->{'REVERSE_FLOW'}}];
}
sub JobNameList{
    my ($class) = @_;
    return [keys %{ $class->{'REVERSE_JOB'} } ];
}

sub SnapJobList{
    my ($class) = @_;
    my $ref = [keys %{ $class->{'JOBSNAP'} }];
    return $ref;
}
sub getFlowId{
    my ($class, $flow_nm) = @_;
    return $class->{'REVERSE_FLOW'}->{$flow_nm};
}
sub getJobId{
    my ($class, $name) = @_;
    return $class->{'REVERSE_JOB'}->{$name};
}

sub queryJob{
    # para: type 查询类型, 目前支持JOBSNAP, JOBHIS两种sql
    #       para_ref  查询条件  针对JOBHIS中需要的参数
    # return: snap 数据结构 hash引用
    # Update: 2014-09-22
    my ($class, $type, $para_ref) = @_;

    my $key_arr;
    my $query_sql = {};

    if ( $type eq "JOBSNAP" )
    {
        ## etl_job 查询
        $query_sql->{'JOBSNAP'} = "select job.id, job.NAME, LAST_JOBSTATUS, LAST_TXDATE, LAST_STARTTIME, LAST_ENDTIME "
                   ." from etl_job job inner join etl_sys etlsys "
                   ." on job.sys_id=etlsys.id inner join job_addition add on add.id=job.id "
                   ." inner join system sys on sys.system_id=add.system_id where sys.system_abbr_nm ='"
                   ."$class->{'PARA'}->{'ETLSYS'}"
                   ."' with ur";
        ## 针对作业快照设置间隔时间，，使用对象缓存，避免频繁查询数据库
        if ( $class->{'PARA'}->{'LASTUPTM'} )
        {
            return if ( time() - $class->{'PARA'}->{'LASTUPTM'} < $class->{'PARA'}->{'SNAPMIN'} );
        }
        # 设置查询返回结果的字段名称,第一个字段用作主键
        $key_arr = [ '01_NAME', '02_LAST_JOBSTATUS', '03_LAST_TXDATE', '04_LAST_STARTTIME', '05_LAST_ENDTIME  ' ];
        $class->{'PARA'}->{'LASTUPTM'} = time();
    }
    elsif ( $type eq "JOBHIS" )
    {
        ## etl_job_log 查询由各基本条件组合而成,通过传入参数决定是否采用某些查询条件，最终拼合成完成sql
        my ( $query_base, $query_date, $query_job, $query_time, $query_limit ) = ('', '', '', '', '');

        $query_base = "select job.name, his.txdate, his.starttime, his.endtime, his.returncode from etl_job_log his "
                   ." inner join etl_job job on his.job_name=job.name"
                   ." inner join etl_sys etlsys on job.sys_id=etlsys.id"
                   ." inner join job_addition add on add.id=job.id"
                  # ." inner join job_flow flow on add.flow_id=flow.flow_id"
                   ." inner join system sys on sys.system_id=add.system_id"
                   ." where sys.system_abbr_nm='"
                   ."$class->{'PARA'}->{'ETLSYS'}"
                   ."' ";
        $query_date = "and his.txdate='"
                   ."$para_ref->{'TXDATE'}"
                   ."' " if ( $para_ref->{'TXDATE'} );
        $query_job = "and job_name='"
              ."$para_ref->{'JOB_NAME'}"
              ."' " if ( $para_ref->{'JOB_NAME'} );
        $query_time = "and ((his.starttime between '"
               ."$para_ref->{'STARTTIME'}"
               ."' and '"
               ."$para_ref->{'ENDTIME'}"
               ."' or his.endtime between '"
               ."$para_ref->{'STARTTIME'}"
               ."' and '"
               ."$para_ref->{'ENDTIME'}"
               ."' ) "
               ." or (his.starttime < '"
               ."$para_ref->{'STARTTIME'}"
               ."' and his.endtime > '"
               ."$para_ref->{'ENDTIME'}"
               ."') )" if ( $para_ref->{'STARTTIME'} and $para_ref->{'ENDTIME'} );

       $query_limit = "order by starttime desc fetch first 3000 row only with ur";

        $query_sql->{'JOBHIS'} .= $query_base.$query_job.$query_date.$query_time.$query_limit;

        #$key_arr = ['FLOW_ID', 'JOB_ID', 'STARTTIME', 'ENDTIME', 'RETURNCODE'];
    }
    ## 继承方法调用
    my $ref = $class->queryDB($query_sql->{$type}, $key_arr);
    return $ref;
}

sub updateSnap{
    my ($class, $snap) = @_;
    $class->{'JOBSNAP'} = $snap if ( $snap );

    #更新快照后同时更新流日期
    $class->syncFlowDate();
}

sub syncFlowDate{
    my ($class) = @_;
    my $flow_list = $class->getFlowList();
    foreach ( @$flow_list )
    {
        my $flow_info = $class->getBaseInfo("FLOW", $_);
        next unless ( $flow_info->{'04_FLOW_DATE'} );
        #next if ( $flow_info->{'ENABLE'} eq "1" );
        my $updt_job = $flow_info->{'05_UPDT_JOB'};
        next unless ( $updt_job );
        my $updt_job_id = $class->getJobId($updt_job);
        my $updt_txdate = $class->getBaseInfo("JOBSNAP", $updt_job_id, "03_LAST_TXDATE");
        next unless ( $updt_txdate );

        my $next_txdate = $updt_txdate;
        if ( $class->getBaseInfo("JOBSNAP", $updt_job_id, "02_LAST_JOBSTATUS") eq "Done" )
        {
            $next_txdate = public::TimetoChar(public::ChartoTime($updt_txdate) + 86400);
        }

        #$flow_info->{'FLOW_DATE'} = $next_txdate;
        $class->{'FLOW'}->{$_}->{'04_FLOW_DATE'} = $next_txdate;
    }
}

sub groupSort{
    # PARA: 
    # $job_list( ref of list job's id )
    # $sort_type ( FLOW, TXDATE, SYSNAME )
    # return: hash ref(flow_id->[job_id,])

    my ($class, $job_list, $sort_type) = @_;
    my $sort_ref = {};

    if ( $sort_type eq "FLOW" )
    {
        foreach my $job_id ( @{ $job_list }  )
        {
            my $job_info = $class->getBaseInfo('JOB',$job_id);
            my $flow_nm = $job_info->{'03_FLOW_NM'};
            next unless ( $flow_nm );

            if ( not exists $sort_ref->{$flow_nm} )
            {
                $sort_ref->{$flow_nm} = [];
            }
            push @{ $sort_ref->{$flow_nm} }, $job_id;
        }
    }
    if ( $sort_type eq "TXDATE" )
    {
        foreach my $job_id ( @{ $job_list } )
        {
            my $job_snap = $class->getBaseInfo('JOBSNAP', $job_id);
            my $dt = $job_snap->{'03_LAST_TXDATE'};
            next unless ( $dt );
            if ( not exists $sort_ref->{$dt} )
            {
                $sort_ref->{$dt} = [];
            }
            push @{ $sort_ref->{$dt} }, $job_id;
        }
    }
    if ( $sort_type eq "STATUS" )
        {
        foreach my $job_id ( @{ $job_list } )
        {
            my $job_snap = $class->getBaseInfo('JOBSNAP', $job_id);
            my $status = $job_snap->{'02_LAST_JOBSTATUS'};
            $status = "None" unless ( $status );
            if ( not exists $sort_ref->{$status} )
            {
                $sort_ref->{$status} = [];
            }
            push @{ $sort_ref->{$status} }, $job_id;
        }
    }
    return $sort_ref;
}

sub flow_tail_jobs{
    my ( $class, $flow_name ) = @_;
    my $tail_jobs = [];
    
    my $sortByFlow = $class->groupSort( [ keys %{$class->{'JOB'}} ], 'FLOW');
    foreach my $id ( @{ $sortByFlow->{$flow_name} } )
    {
        my $down_dep = $class->getDepDown($id);
        my $down_trig = $class->getTrigDown($id);
        
        my $is_tail_job = 1;
        foreach my $down_id ( @$down_dep, @$down_trig )
        {
            my $flow_nm = $class->getBaseInfo('JOB', $down_id)->{'03_FLOW_NM'};
            if ( $flow_nm eq $flow_name )
            {
                $is_tail_job = 0;
                last;
            }
        }
        push @$tail_jobs, $id if ( $is_tail_job );
    }
    return $tail_jobs;
}

sub flow_head_jobs{
    my ( $class, $flow_name ) = @_;
    my $head_jobs = [];
    
    my $sortByFlow = $class->groupSort( [ keys %{ $class->{'JOB'}} ], 'FLOW');
    foreach my $id ( @{ $sortByFlow->{$flow_name} } )
    {
        my $up_dep = $class->getDepUp($id);
        my $up_trig = $class->getTrigUp($id);
        
        my $is_head_job = 1;
        foreach my $up_id ( @$up_dep, @$up_trig )
        {
            my $flow_nm = $class->getBaseInfo('JOB', $up_id)->{'03_FLOW_NM'};
            if ( $flow_nm eq $flow_name )
            {
                $is_head_job = 0;
                last;
            }
        }
        push @$head_jobs, $id if ( $is_head_job );
    }
    return $head_jobs;
}    

sub flow_date_expect{
    ## 流日期预测： 在无流日期时根据收尾作业日期预测期望的流日期，以此作为所有作业的执行目标
    ## 规则:  末尾作业日期有不等情况，以最大值为流期望日期（有效作业）
    ##        末尾作业相等情况，作业状态为done则tx_date+1为流期望日期
    my ( $class, $flow_name ) = @_;
    
    my $flow_id = $class->getFlowId($flow_name);
    my $flow_date = $class->getBaseInfo("FLOW", $flow_id, "04_FLOW_DATE");
    chomp $flow_date;
    
    if ( $flow_date  =~ /\d+/ )
    {
        return $flow_date;
    }
    else
    {
        my $tail_jobs = $class->flow_tail_jobs($flow_name);
        my $stand_flow_date;
        foreach my $id ( @$tail_jobs )
        {
            my $job_status = $class->getBaseInfo('JOBSNAP', $id)->{'02_LAST_JOBSTATUS'};
            my $job_txdate = $class->getBaseInfo("JOBSNAP", $id)->{'03_LAST_TXDATE'};

            if ( $job_status eq "Done" )
            {
                my $expect_dt = public::TimetoChar(public::ChartoTime($job_txdate) + 86400);
                $stand_flow_date = $expect_dt unless ( $stand_flow_date );
                if ( $expect_dt < $stand_flow_date )
                {
                    $stand_flow_date = $expect_dt;
                }
            }
            elsif ( $job_status eq "Running" or $job_status eq "Pending" )
            {
                $stand_flow_date = $expect_dt;
            }
        }
        #print "expect=$stand_flow_date\n";
        return $stand_flow_date;
    }
}
####end 2015-06-29


sub stageShow{
## 内置上、下挂角及普通行格式，将传入的$rows_ref列表引用内容按照段落输出
## 获取当前定义装饰器，及待输出的列表引用
## 按照预定格式输出带段落前导符号的内容
## 参数: $rows_ref 最终整个段落内容的列表引用 [ [row1], [row2], [row3] ... ]

    my ($class, $rows_ref, $decor ) = @_;
    my $all_count = scalar( @$rows_ref );
    
    if ( $all_count <= 1 )
    {
        my $fF = fullcorFormat->new();
        $fF->decorate($class);
        if ( $decor )
        {
            $decor->decorate($fF);
            $decor->showing($rows_ref->[0]);
        }
        else
        {
            $fF->showing($rows_ref->[0]);
        }
    }
    
    if ( $all_count > 1 )
    {
        foreach (1..$all_count)
        {
            if ( $_ == 1 )
            {
                my $uF = upcorFormat->new();
                $uF->decorate($class);
                if ( $decor )
                {
                    $decor->decorate($uF);
                    $decor->showing($rows_ref->[$_-1]);
                }
                else
                {
                    $uF->showing($rows_ref->[$_-1]);
                }
                next;
            }
            if ( $_ == $all_count )
            {
                my $dF = downcorFormat->new();
                $dF->decorate($class);
                if ( $decor )
                {
                    $decor->decorate($dF);
                    $decor->showing($rows_ref->[$_-1]);
                }
                else
                {
                    $dF->showing($rows_ref->[$_-1]);
                }
                next;
            }

            my $rF = rowFormat->new();
            $rF->decorate($class);
            if ( $decor )
            {
                $decor->decorate($rF);
                $decor->showing($rows_ref->[$_-1]);
            }
            else
            {
                $rF->showing($rows_ref->[$_-1]);
            }
        }
    }
}

sub createSegment{
    my ( $class, $seg, $decor ) = @_;
    
    my $sF = spaceFormat->new();   #空格格式
    my $uF = upcorFormat->new();   #上挂角   ┌
    #my $rF = rowFormat->new();     #块行格式 │
    my $bF = downcorFormat->new(); #下挂角   └
      
    
    $uF->decorate($class);
    ( $decor )? $decor->decorate($uF) : $decor = $uF;
    $decor->showing();
    
    foreach my $k ( @$seg )
    {
        
        if ( ref($k->[0]) )
        {
            my $rF = rowFormat->new();     #行格式 │
            $rF->decorate($decor);
            $class->createSegment($class, $k, $rF);     #子块外部统一增加装饰:rowFormat
        }
        else
        {
            my $rF = rowFormat->new();
            $rF->decorate($class);
            ( $decor )? $decor->decorate($rF) : $decor = $rF;
            $decor->showing($k);
        }
    }

    $bF->decorate($class); 
    ( $decor )? $decor->decorate($bF) : $decor = $bF;
    $decor->showing();
}

sub createSegment2{
    ### 将传入的列表引用整合为段落格式，返回列表
    my ( $class, $title, $seg, $decor ) = @_;
    my $whole = [];

    ## 段落标题格式    
    my $uF = upcorFormat->new();   #上挂角   ┌
    $uF->decorate($class);
    my $up;
    if ( $decor )
    {
         $up = $decor;
         $up->decorate($uF);
    }
    else
    {
         $up = $uF;
    }
    push @$whole, $up->combing($title);
    
    ## 段落行格式
    my $rF = rowFormat->new();     #行格式 │
    $rF->decorate($class);
    foreach my $k ( @$seg )
    {
        my $mid;
        if ( $decor )
        {
             $mid = $decor;
             $mid->decorate($rF);
        }
        else
        {
            $mid = $rF;
        }
        push @$whole, $mid->combing($k);
    }
    
    ## 段落末尾格式
    my $bF = downcorFormat->new(); #下挂角   └
    $bF->decorate($class);
    my $dn;
    if ( $decor )
    {
         $dn = $decor; 
         $dn->decorate($bF);
    }
    else
    {
        $dn = $bF;
    }
    push @$whole, $dn->combing($k);
    
    return $whole;  #最终合成的一个列表段落
}

sub showing{
    my ( $class, $row_ref ) = @_;
    
    foreach ( @$row_ref )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  ";
    }
    print "\n";
}
sub combing{
    my ( $class, $row, $whole ) = @_;
    
    ( ref($row) )? push @$whole, @$row : push @$whole, $row; 

    return $whole;
}

## 作业检查检查
sub jobHealth{
    ##  <检查作业执行状态>
    ##  1.输入参数： $job_id   根据给定的作业，追溯导致指定job未完成的上游作业
    ##               $stand_date 基准比较日期， 有流日期情况，以流日期为基准，判断作业是否完成
    ##                           无流日期情况，以传入日期为基准，判断作业是否完成
    ##               $chked_job, $run_queue, $wait_queue 临时全局变量，用于在整个分析过程中保存
                  
    my ( $class, $job_id, $chked_job, $run_queue, $wait_queue, $stand_date ) = @_;


    return 1 if ( exists $chked_job->{$job_id} );   #避免陷入死循环，检查过的作业跳过
    $chked_job->{$job_id} = 1;

    ## 检查该job所属流是否有flow_date, 如无，则以stand_date为标准
    my $cur_status = $class->getBaseInfo("JOBSNAP", $job_id, "02_LAST_JOBSTATUS");
    my $cur_txdate = $class->getBaseInfo("JOBSNAP", $job_id, "03_LAST_TXDATE");
    #my $flow_nm = $class->getBaseInfo("JOB", $job_id)->{'03_FLOW_NM'};
    #my $flow_id = $class->getFlowId($flow_nm);
    #my $flow_dt = $class->getBaseInfo("FLOW", $flow_id)->{'04_FLOW_DATE'};
    

    return 0 if ($cur_txdate eq $stand_date && $cur_status eq "Done" );

    if ( $cur_status eq "Running" )
    {
        push @$run_queue, $job_id;
        return 1;
    }
    if ( $cur_status eq "Failed" or $cur_status eq "Pending")
    {
        push @$wait_queue, $job_id;
        return 1;
    }

    if ( !($cur_status) or !($cur_txdate)  or  $cur_txdate < $stand_date )
    {
        my $all_job_up = [];
        my $trig_up = $class->getTrigUp($job_id);
        my $dep_up = $class->getDepUp($job_id);
        push @$all_job_up, @$trig_up, @$dep_up;

        ## 源文件触发情况
        unless ( scalar @$all_job_up )
        {
            if ( my $sorce_file = $class->getBaseInfo("JOB", $job_id, "08_FILE_NM") )
            {
                #print "$cur_status---- $cur_txdate ---- $stand_date\n";
#                print $class->getBaseInfo('JOB', $job_id, "01_NAME"),'   ', $class->getBaseInfo('JOBSNAP', $job_id, '03_LAST_TXDATE'), "\n";
                
                push @$wait_queue, $job_id;
            }
            return 1;
        }

        my $return_code = 0;
        foreach my $id ( @$all_job_up )
        {
     
            my $rt = $class->jobHealth($id, $chked_job, $run_queue, $wait_queue, $stand_date);
            $return_code += $rt;

        }
        if ( $return_code == 0 )
        {
            push @$wait_queue, $job_id ;
            return 1;
        }
        return 1;
    }
}


sub jobHealth_20150616{
    my ( $class, $job_id, $stand_txdate, $chked_job, $run_queue, $wait_queue ) =  @_;
    
    my $cur_status = $class->getBaseInfo("JOBSNAP", $job_id, "02_LAST_JOBSTATUS");
    my $cur_txdate = $class->getBaseInfo("JOBSNAP", $job_id, "03_LAST_TXDATE");
    
    return "OK" if ( exists $chked_job->{$job_id} );   #避免陷入交叉循环，检查过的作业跳过
    
    if ( $cur_status eq "Running" ) ### 忽略pending## or $cur_status eq "Pending" )
    {
        push @$run_queue, $job_id;
        $chked_job->{$job_id} = 1;
        return "Run";
    }
    if ( $cur_status eq "Failed" )
    {
        push @$wait_queue, $job_id;
        $chked_job->{$job_id} = 1;
        return "None";
    }
    
    if ( !($cur_status)  or  !($cur_txdate)  or  $cur_txdate < $stand_txdate ) 
    {
        my $all_job_up = [];
        my $trig_up = $class->getTrigUp($job_id);
        my $dep_up = $class->getDepUp($job_id);
        push @$all_job_up, @$trig_up, @$dep_up;
        
        ## 源文件触发情况
        unless ( scalar @$all_job_up )
        {
            if ( my $sorce_file = $class->getBaseInfo("JOB", $job_id, "08_FILE_NM") )
            {
                push @$wait_queue, $job_id;
            }
        }
        my $all_job_ok;
        foreach ( @$all_job_up )
        {
            $all_job_ok = $class->jobHealth($_, $stand_txdate, $chked_job, $run_queue, $wait_queue);
        }
        push @$wait_queue, $job_id if ( $all_job_ok eq "Done" );
        $chked_job->{$job_id} = 1;
        
        return "None";
    }
    
    $chked_job->{$job_id} = 1;
    return "OK";
}

sub showModel{
    my ( $class ) = @_;

    print Dumper($class);
}

1;
