#!/usr/bin/perl

use FindBin qw($Bin);
use Data::Dumper;
use POSIX qw(mktime);
use Term::Complete;

use Version;
use EtlSource;
use Etm;
use lib "$Bin";
use public;

$VERSION = 4.10;
use constant PROJECT => "ETLNEXT";


#################################
sub main;
sub prepareModel;
sub localModel;
sub menuHistory;

sub Menu_Global;
sub Menu_JobInfo;
sub Menu_FlowInfo;
sub Menu_ReBuild;
sub Menu_FlowPro;

use constant MODEL_DIR => $Bin."/models";


my $modules = ['main', 'Version', 'EtlSource', 'Etm', 'CusFormat'];
my $version = Version->new();
foreach ( @$modules )
{
    $version->addModule($_, $_->VERSION);
}
$version->syncVer(PROJECT);

main;



########## 方法定义 ##########

sub main{
    my $model = Etm->new();

    ## 模型准备过程
    prepareModel($model, $ARGV[0]);

    ## 创建主菜单,初始化个数
    my $menu = {};
    #foreach (1..3)
    #{
    #    $menu->{$_} = [];
    #}
    ## 将菜单各项与具体处理的方法关联
    push @{$menu->{'0'}}, \&Menu_Global, "0. 作业执行一览";
    push @{$menu->{'1'}}, \&Menu_JobInfo, "1. 查询作业定义";
    push @{$menu->{'2'}}, \&Menu_FlowInfo, "2. 查询流定义";
    push @{$menu->{'5'}}, \&Menu_FlowPro, "5. 流执行诊断";
    push @{$menu->{'9'}}, \&Menu_ReBuild, "9. 更新本地模型";


    ## 主菜单，循环接收输入
    while ( 1 )
    {
        system("clear");
        print "\tETLPLUS Metal Gear\n\n";
        foreach my $key (sort keys %$menu)
        {
            print $menu->{$key}->[1], "\n";
        }
        print "     please input a choice:  ";
        my $choice = <STDIN>;
        chomp $choice;
        exit if ( uc($choice) eq "X" );

        if ( exists $menu->{$choice} )
        {
        my $snap = $model->queryJob("JOBSNAP");
        $model->updateSnap($snap);
        $menu->{$choice}->[0]($model);
        }
        print "press any key to continue......\n";
        <STDIN>;
    }
    
}



##### 准备模式 ( 获取输入参数，检索本地模型文件，初始化,装载模型   )
sub prepareModel{
    my ($model, $sysname) = @_;

    ## 获取本地模型清单
    my $modelHistory = localModel(MODEL_DIR);

    ## 如无输入参数$sysname, 展示菜单获取输入
    $sysname = receiveModel($modelHistory) unless ( $sysname );
    
    if ( exists $modelHistory->{$sysname} )
    {
        ## 存在历史模型,直接装载
        $model->loadModel(MODEL_DIR."/".$sysname, $sysname);
    }
    else
    {
        ## 初始化新模型,装载
        $model->initModel($sysname);
        $model->loadModel(MODEL_DIR."/".$sysname, $sysname);
    }

    print "build job list...\n";
    print "build flow list...\n";

}

##### 装载模型
sub localModel{
    my ( $model_dir ) = shift;
    my $sys_ref = {};

    ## 获取本地
    opendir DH, $model_dir;
    foreach ( readdir DH )
    {
        next if ( /^\./ );
        my $etl_sys_nm = $_;
        my $IdFile = $model_dir."/".$etl_sys_nm."/".$etl_sys_nm.".id";
        next unless ( -r $IdFile );
        $sys_ref->{$etl_sys_nm} = $IdFile;
    }
    return $sys_ref;
}

sub receiveModel{
    my ($list_ref) = shift;

    ## 打印本地模型清单
    print "Local Etl Model List:\n" if ( scalar(keys %$sys_ref) > 0);   
    foreach my $sys_nm ( keys %$list_ref )
    {
        print "  $sys_nm\n";
    }

    ## 获取输入系统名
    print "Please input an sysname, \'X' for exit:\n";
    while ( <STDIN> )
    {
        next if ( $_ eq "\n" );
        chomp;
        my $input = $_;
        exit if ( uc($input) eq "X");
        return $input;
    }
}


#####  菜单函数 #####
sub Menu_Global{
    my ($object) = @_;

    my $job_list = [ keys %{$object->{'JOB'}} ];
    
    my $job_block = [];
    my $grpByStat = $object->groupSort($job_list, 'STATUS');
    my $grpByDate = $object->groupSort($job_list, 'TXDATE');
    foreach my $txdate( reverse sort keys %{$grpByDate} )
    {
        my $str = [];
        push @$str, $txdate;
        my $divByStat = $object->groupSort( $grpByDate->{$txdate}, 'STATUS');
        foreach my $stat ( sort keys %{$divByStat} )
        {
            push @$str, $stat."(".scalar( @{$divByStat->{$stat}} ).")";
        }
        push @$job_block, $str;
    }

    
    ### 块格式定义
    my $sF = spaceFormat->new();   #空格格式
    my $tF = upcorFormat->new();   #上挂角
    my $bF = downcorFormat->new(); #下挂角
    my $rF = rowFormat->new();     #块行格式
    $sF->decorate($object);        #起始空格格式

    ## 创建并打印按txdate分类统计格式段
    my $dis_txdate = $object->createSegment2("Txdate", $job_block, $sF);   #为整个段落额外增加空格格式
    foreach my $line ( @$dis_txdate )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    }

    ## 2. running,pending,fail作业列表
    my $all_stat_block = [];
    foreach my $st ( keys %{$grpByStat} )
    {
        my $stat_block = [];
        next if ( $st eq "Done" );
        next if ( $st eq "None" );

        foreach ( @{$grpByStat->{$st}} )
        {
            my $str = [];
            my $snap = $object->getBaseInfo("JOBSNAP", $_);
            foreach my $k ( sort keys %$snap )
            {
                if ( $k =~ /NAME/ )
                {
                    my $sp = 50 - length($snap->{$k});
                    push @$str, $snap->{$k} . " "x$sp;
                }
                else
                {
                    push @$str, $snap->{$k};
                }
            }            
            my $flow_nm = $object->getBaseInfo("JOB", $_, "03_FLOW_NM");
            push @$str, $flow_nm;

            push @$stat_block, $str;
       } 
       my $dis_stat = $object->createSegment2("[$st]", $stat_block);
       push @$all_stat_block, @$dis_stat;
    }
    my $dis_all_stat = $object->createSegment2("[Current]", $all_stat_block, $sF);
    foreach my $line ( @$dis_all_stat )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    }
    
}

sub Menu_JobInfo{
    my ($object) = @_;

    my $jobList = $object->JobNameList();
    #my $snap = $object->queryJob("JOBSNAP");
    #$object->updateSnap($snap);

    print "Input JobName:";
    my $jobName = public::AInput($jobList);
    print "\n";
    next unless ($jobName);

    my $job_id = $object->getJobId($jobName);

    ### 生成作业基础信息块
    my $job_detail = [];
    my $job_hash = $object->getBaseInfo("JOB", $job_id);   
    foreach ( sort keys %$job_hash )  ## 将关于作业的基础信息按照预期显示，保存为列表
    {
        my $str = [];
        push @$str, substr($_,3);     ## 此处与内置数据的名称有密切关联， substr将字段名前的标号01_去除
        push @$str, ":  ";
        push @$str, $job_hash->{$_};

        push @$job_detail, $str;
    }

    ###  生成作业最新执行日志信息块
    my $log_detail = [];
    my $joblog = $object->getBaseInfo("JOBSNAP", $job_id);
    foreach ( sort keys %$joblog ) 
    {
        my $str = [];
        push @$str, substr($_,3);
        push @$str, " :  ";
        push @$str, $joblog->{$_};

        push @$log_detail, $str;
    }
    
    ### 块格式定义
    my $sF = spaceFormat->new();   #空格格式
    my $tF = upcorFormat->new();   #上挂角
    my $bF = downcorFormat->new(); #下挂角
    my $rF = rowFormat->new();     #块行格式
    $sF->decorate($object);        #起始空格格式

    ## A. 打印作业基础信息，作业执行信息
    my $whole = $object->createSegment2("[Job Description]", $job_detail, $sF);   #为整个段落额外增加空格格式
    foreach my $line ( @$whole )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    }
    $whole = $object->createSegment2("[Run Log]", $log_detail, $sF);
    foreach my $line ( @$whole )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    }

    ## 分析依赖触发
    my $job_dep_up = $object->getDepUp($job_id);
    my $job_trig_up = $object->getTrigUp($job_id);
    my $job_dep_down = $object->getDepDown($job_id);
    my $job_trig_down = $object->getTrigDown($job_id);


    my $relation_ref = [];    #将触发依赖关系保存为列表，每行第一位为关系名称，第二位为关系ref. 便于后续轮询处理
    my $rela_type = [];
    push @$rela_type, "UP触发作业:", $job_trig_up if ( $job_trig_up );
    push @$relation_ref, $rela_type if ( $job_trig_up );
    $rela_type = [];
    push @$rela_type, "UP依赖作业:", $job_dep_up if ( $job_dep_up );
    push @$relation_ref, $rela_type if ( $job_dep_up );
    $rela_type = [];
    push @$rela_type, "DOWN触发作业:", $job_trig_down if ( $job_trig_down );
    push @$relation_ref, $rela_type if ( $job_trig_down );
    $rela_type = [];
    push @$rela_type, "DOWN依赖作业:", $job_dep_down if ( $job_dep_down );
    push @$relation_ref, $rela_type if ( $job_dep_down );
    undef $rela_type;

    my $rela_detail = [];    #保存依赖触发关系输出段
    
#    if ( my $file_nm = $object->getBaseInfo("JOB", $job_id, "08_FILE_NM") )
#    {
#        my $str = [];
#        push @$str, "文件触发:";
#        push @$rela_detail, $str;

#        $str = [];
#        push @$str, "    ";
#        push @$str, $file_nm;
#        push @$rela_detail, $str;
#    }

#    foreach ( @$relation_ref )
#    {
#        my $str = [];
#        push @$str, $_->[0];
#        push @$rela_detail, $str;
#        
#        foreach my $id ( @{$_->[1]} )
#        {
#        $str = [];
#        my $job_name = $object->getBaseInfo("JOB", $id, "01_NAME");
#        my $flow_nm = $object->getBaseInfo("JOB", $id, "03_FLOW_NM");
#        my $stat = $object->getBaseInfo("JOBSNAP", $id, "02_LAST_JOBSTATUS");
#        my $txdate = $object->getBaseInfo("JOBSNAP", $id, "03_LAST_TXDATE");
#        
#        push @$str, "    ";
#        my $add_space = " "x(50-length($job_name));
#        push @$str, $job_name.$add_space."  ";
#        push @$str, $stat."    ";
#        push @$str, $txdate."    ";
#        push @$str, $flow_nm;
        
#        push @$rela_detail, $str;
#        }
#    }

    my $whole_rela = [];
    if ( my $file_nm = $object->getBaseInfo("JOB", $job_id, "08_FILE_NM") )
    {
        my $str = [];
        push @$str, $file_nm;
        
        my $rt = $object->createSegment2("[File Trigger]", $str, $sF);
        
        push @$whole_rela, @$rt;
    }
    foreach ( @$relation_ref )
    {
        my $seg = [];
        my $title = $_->[0];
        
        foreach my $id ( @{$_->[1]} )
        {
        $str = [];
        my $job_name = $object->getBaseInfo("JOB", $id, "01_NAME");
        my $flow_nm = $object->getBaseInfo("JOB", $id, "03_FLOW_NM");
        my $stat = $object->getBaseInfo("JOBSNAP", $id, "02_LAST_JOBSTATUS");
        my $txdate = $object->getBaseInfo("JOBSNAP", $id, "03_LAST_TXDATE");

        my $add_space = " "x(50-length($job_name));
        push @$str, $job_name.$add_space;
        push @$str, $stat;
        push @$str, $txdate;
        push @$str, $flow_nm;

        push @$seg, $str;
        }
        
        my $rt = $object->createSegment2("[$title]", $seg);
        push @$whole_rela, @$rt;
    }    
    ## B. 打印作业依赖触发关系块
    my $whole = $object->createSegment2("[Relation]", $whole_rela, $sF);
    foreach my $line ( @$whole )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    }

    ## 暂停是否继续处理
    print "continue(n) show job history log or break(any other key):";
    my $ch = <STDIN>;
    chomp $ch;
    next unless ( uc($ch) eq "N" );
    
    ## 作业历史日志 块 
    my $para = {};
    $para->{'JOB_NAME'} = $jobName;
    my $job_his = $object->queryJob("JOBHIS", $para);
        
    my $his_window = 30;  # 每个区块包含多少条日志
    my $his_all = scalar(@$job_his);

    my $step = 0;
    while(1)
    {
        my $pop = [];
        @$pop = @$job_his[$step..$step+$his_window];  # 对完整列表分片，按窗口大小取
        my $whole = $object->createSegment2("[Job History]", $pop, $sF);
        foreach my $line ( @$whole )
        {
            ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
            print "\n";
        }


        $step += $his_window+1;
        last if ( $step > $his_all );
        print "continue(n) or break(any other key):";
        my $k = <STDIN>;
        chomp $k;
        next if ( uc($k) eq "N" );
        last;
    }

}

sub Menu_FlowInfo{
    my ($object) = @_;

    my $flowList = $object->FlowNameList();                 
#    my $snap = $object->queryJob("JOBSNAP");                
#    $object->updateSnap($snap);                             
                                                        
    print "Input FlowName:";                                
    my $flowName = public::AInput($flowList);                       
    print "\n";                                             
    next unless ($flowName);                                
    system('clear');                                                   

    ## 1.根据输入获取流信息
    my $flow_id = $object->getFlowId($flowName);            
    my $flow_block = [];                                   
    my $flow_hash = $object->getBaseInfo("FLOW", $flow_id); 
    foreach ( sort keys %$flow_hash )                       
    {                                                       
        my $str = [];                                       
        push @$str, substr($_, 3);                          
        push @$str, ": ";                                  
        push @$str, $flow_hash->{$_};                       
                                                        
        push @$flow_block, $str;                           
    }                                                       

    my $sF = spaceFormat->new();   #空格格式
    my $tF = upcorFormat->new();   #上挂角
    my $bF = downcorFormat->new(); #下挂角
    my $rF = rowFormat->new();     #块行格式
   # $sF->decorate($object);        #起始空格格式

    ## 创建并打印流基础信息格式段
    my $dis_flow_basic = $object->createSegment2("[Flow Description]", $flow_block, $sF);   #为整个段落额外增加空格格式
    foreach my $line ( @$dis_flow_basic )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    }

    ## 2.分析流作业日期分类统计
    my $job_list = [ keys %{$object->{'JOB'}} ];  #全部作业id

    my $jobgrp_block = [];

    my $grpByFlow = $object->groupSort($job_list, 'FLOW');
    my $grpByDate = $object->groupSort($grpByFlow->{$flowName}, 'TXDATE');
    foreach my $txdate( keys %{$grpByDate} )
    {
        my $str = [];
        push @$str, $txdate;
        my $grpByStat = $object->groupSort( $grpByDate->{$txdate}, 'STATUS');
        foreach my $stat ( sort keys %{$grpByStat} )
        {
            push @$str, $stat."(".scalar( @{$grpByStat->{$stat}} ).")";
        }
        push @$jobgrp_block, $str;
    }

    my $dis_flow_txdate = $object->createSegment2("[Txdate]", $jobgrp_block, $sF);   #为整个段落额外增加空格格式
    foreach my $line ( @$dis_flow_txdate )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    } 

    ### 3.作业诊断
    #my $flow_dt = $object->getBaseInfo("FLOW", $flow_id, "04_FLOW_DATE");
    #my $updt_job = $object->getBaseInfo("FLOW", $flow_id, "05_UPDT_JOB");
    #my $updt_jobid = $object->getJobId($updt_job);

    # 获取流末尾作业
    my $tail_jobs = $object->flow_tail_jobs($flowName);
    my $flowdt_expect = $object->flow_date_expect($flowName);

    my $chked_job = {};
    my $run_queue = [];
    my $wait_queue = [];

    foreach ( @$tail_jobs )
    {
        $object->jobHealth($_, $chked_job, $run_queue, $wait_queue, $flowdt_expect);
    }

    my $run_block = [];

    foreach my $run_id ( @$run_queue )
    {
        my $str = [];
        my $run_snap = $object->getBaseInfo("JOBSNAP", $run_id);
        push @$str, "+";
        foreach ( sort keys %$run_snap )
        {
            if ( $_ =~ /NAME/ )
            {
                my $sp = 50 - length($run_snap->{$_});
                push @$str, $run_snap->{$_} . " "x$sp;
            }
            else
            {
                push @$str, $run_snap->{$_};
            }
        }
        my $flow_nm = $object->getBaseInfo("JOB", $run_id, "03_FLOW_NM");
        push @$str, $flow_nm;
        push @$run_block, $str;
        undef $str;

        # 检索该作业所触发下级作业
        my $next_trig = $object->getTrigDown($run_id);
        if ( $next_trig ){
            foreach my $id (  @$next_trig )
            {
                my $snap = $object->getBaseInfo("JOBSNAP", $id);
                push @$str, " -->";
                foreach my $k ( sort keys %$snap )
                {
                    if ( $k =~ /NAME/ )
                    {
                         my $sp = 50 - length($snap->{$k});
                         push @$str, $snap->{$k} . " "x$sp;
                    }
                    else
                    {
                        next if ( $k =~ /TIME/ );
                        push @$str, $snap->{$k};
                    }
                }
                my $flow_nm = $object->getBaseInfo("JOB", $id, "03_FLOW_NM");
                push @$str, $flow_nm;
                push @$run_block, $str;
                undef $str;
            }
        }
    }
    # 创建执行队列信息段
    my $dis_run_block = $object->createSegment2("[Run Queue]", $run_block);

    ## 分析等待队列作业信息
    my $wait_block = [];
    foreach my $wait_id ( @$wait_queue )
    {
        my $str = [];
        my $wait_snap = $object->getBaseInfo("JOBSNAP", $wait_id);

        if ( my $source_file = $object->getBaseInfo("JOB", $wait_id, "08_FILE_NM") )
        {
            # 文件触发
            my $job_nm = $object->getBaseInfo("JOB", $wait_id, "01_NAME");
            my $sp = ' ' x (50 - length($job_nm));
            $job_nm .= $sp;
            my $txdate = $object->getBaseInfo("JOBSNAP", $wait_id, "03_LAST_TXDATE");

            my $flow_nm = $object->getBaseInfo("JOB", $wait_id, "03_FLOW_NM");
            my $flow_id = $object->getFlowId($flow_nm);
            my $flow_dt = $object->getBaseInfo("FLOW", $flow_id, "04_FLOW_DATE");
        
            $source_file =~ s/\{TX_DATE\}/$flowdt_expect/g;

            push @$str, $job_nm, $txdate, $source_file;
        }
        else
        {
            foreach my $k ( sort keys %$wait_snap )
            {
                if ( $k =~ /NAME/ )
                {
                    my $sp = 50 - length($wait_snap->{$k});
                    push @$str, $wait_snap->{$k} . " "x$sp;
                }
                else
                {
                    push @$str, $wait_snap->{$k};
                }
            }
            my $flow_nm = $object->getBaseInfo("JOB", $wait_id, "03_FLOW_NM");
            push @$str, $flow_nm;
        }
        push @$wait_block, $str;
    }
            
    my $dis_wait_block = $object->createSegment2("[Wait Queue]", $wait_block);

    my $all_block = [];
    push @$all_block, @$dis_run_block, @$dis_wait_block;
    my $dis_all_block = $object->createSegment2("[Job Queue]", $all_block, $sF);
    foreach my $line ( @$dis_all_block )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    }
}


sub Menu_ReBuild{
    my ( $object ) = @_;
  
    my $etlsys = $object->{'PARA'}->{'ETLSYS'};
    
    $object->initModel($etlsys);
    $object->loadModel(MODEL_DIR."/".$etlsys, $etlsys);

#    my $snap = $object->queryJob("JOBSNAP");
#    $object->updateSnap($snap);
    print "\npress any key to return...";
    <STDIN>;
}

sub Menu_FlowPro{
    my ($object) = @_;

    my $job_list = [ keys %{$object->{'JOB'}} ];
    my $grpByFlow = $object->groupSort($job_list, 'FLOW');

    my $flowList = $object->FlowNameList();
    print "Input FlowName:";
    my $flowName = public::AInput($flowList);
    print "\n";
    next unless ($flowName);
    system('clear');

    #my $stand_date;
    #my $flowid = $object->getFlowId($flowName);

    my $flowdt_expect = $object->flow_date_expect($flowName);
    
    my $head_jobs = $object->flow_head_jobs($flowName);
    my $tail_jobs = $object->flow_tail_jobs($flowName);


    my $head_group = [];
    print "head jobs:\n";
    foreach ( @$head_jobs )
    {
        my $item = [];
        my $job_nm = $object->getBaseInfo("JOB", $_)->{'01_NAME'};
        my $sp = 50 - length($job_nm);
        push @$item, $job_nm . ' ' x $sp;
        
        push @$item, $object->getBaseInfo("JOBSNAP", $_)->{'03_LAST_TXDATE'}, $object->getBaseInfo("JOBSNAP", $_)->{'02_LAST_JOBSTATUS'};
        push @$head_group, $item;
    }
    my $sF = spaceFormat->new();
    my $dis_head_jobs = $object->createSegment2("[Flow Header Jobs]", $head_group, $sF);
    foreach my $line ( @$dis_head_jobs )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    }   

    
    my $tail_group = [];
    foreach ( @$tail_jobs )
    {
        my $item = [];
        my $job_nm = $object->getBaseInfo("JOB", $_)->{'01_NAME'};
        my $sp = 50 - length($job_nm);
        push @$item, $job_nm . ' ' x $sp;

        push @$item, $object->getBaseInfo("JOBSNAP", $_)->{'03_LAST_TXDATE'}, $object->getBaseInfo("JOBSNAP", $_)->{'02_LAST_JOBSTATUS'};
        push @$tail_group, $item;
    }
    my $dis_tail_jobs = $object->createSegment2("[Flow Tail Jobs]", $tail_group, $sF);
    foreach my $line ( @$dis_tail_jobs )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    }
}
