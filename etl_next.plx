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



########## �������� ##########

sub main{
    my $model = Etm->new();

    ## ģ��׼������
    prepareModel($model, $ARGV[0]);

    ## �������˵�,��ʼ������
    my $menu = {};
    #foreach (1..3)
    #{
    #    $menu->{$_} = [];
    #}
    ## ���˵���������崦��ķ�������
    push @{$menu->{'0'}}, \&Menu_Global, "0. ��ҵִ��һ��";
    push @{$menu->{'1'}}, \&Menu_JobInfo, "1. ��ѯ��ҵ����";
    push @{$menu->{'2'}}, \&Menu_FlowInfo, "2. ��ѯ������";
    push @{$menu->{'5'}}, \&Menu_FlowPro, "5. ��ִ�����";
    push @{$menu->{'9'}}, \&Menu_ReBuild, "9. ���±���ģ��";


    ## ���˵���ѭ����������
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



##### ׼��ģʽ ( ��ȡ�����������������ģ���ļ�����ʼ��,װ��ģ��   )
sub prepareModel{
    my ($model, $sysname) = @_;

    ## ��ȡ����ģ���嵥
    my $modelHistory = localModel(MODEL_DIR);

    ## �����������$sysname, չʾ�˵���ȡ����
    $sysname = receiveModel($modelHistory) unless ( $sysname );
    
    if ( exists $modelHistory->{$sysname} )
    {
        ## ������ʷģ��,ֱ��װ��
        $model->loadModel(MODEL_DIR."/".$sysname, $sysname);
    }
    else
    {
        ## ��ʼ����ģ��,װ��
        $model->initModel($sysname);
        $model->loadModel(MODEL_DIR."/".$sysname, $sysname);
    }

    print "build job list...\n";
    print "build flow list...\n";

}

##### װ��ģ��
sub localModel{
    my ( $model_dir ) = shift;
    my $sys_ref = {};

    ## ��ȡ����
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

    ## ��ӡ����ģ���嵥
    print "Local Etl Model List:\n" if ( scalar(keys %$sys_ref) > 0);   
    foreach my $sys_nm ( keys %$list_ref )
    {
        print "  $sys_nm\n";
    }

    ## ��ȡ����ϵͳ��
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


#####  �˵����� #####
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

    
    ### ���ʽ����
    my $sF = spaceFormat->new();   #�ո��ʽ
    my $tF = upcorFormat->new();   #�Ϲҽ�
    my $bF = downcorFormat->new(); #�¹ҽ�
    my $rF = rowFormat->new();     #���и�ʽ
    $sF->decorate($object);        #��ʼ�ո��ʽ

    ## ��������ӡ��txdate����ͳ�Ƹ�ʽ��
    my $dis_txdate = $object->createSegment2("Txdate", $job_block, $sF);   #Ϊ��������������ӿո��ʽ
    foreach my $line ( @$dis_txdate )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    }

    ## 2. running,pending,fail��ҵ�б�
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

    ### ������ҵ������Ϣ��
    my $job_detail = [];
    my $job_hash = $object->getBaseInfo("JOB", $job_id);   
    foreach ( sort keys %$job_hash )  ## ��������ҵ�Ļ�����Ϣ����Ԥ����ʾ������Ϊ�б�
    {
        my $str = [];
        push @$str, substr($_,3);     ## �˴����������ݵ����������й����� substr���ֶ���ǰ�ı��01_ȥ��
        push @$str, ":  ";
        push @$str, $job_hash->{$_};

        push @$job_detail, $str;
    }

    ###  ������ҵ����ִ����־��Ϣ��
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
    
    ### ���ʽ����
    my $sF = spaceFormat->new();   #�ո��ʽ
    my $tF = upcorFormat->new();   #�Ϲҽ�
    my $bF = downcorFormat->new(); #�¹ҽ�
    my $rF = rowFormat->new();     #���и�ʽ
    $sF->decorate($object);        #��ʼ�ո��ʽ

    ## A. ��ӡ��ҵ������Ϣ����ҵִ����Ϣ
    my $whole = $object->createSegment2("[Job Description]", $job_detail, $sF);   #Ϊ��������������ӿո��ʽ
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

    ## ������������
    my $job_dep_up = $object->getDepUp($job_id);
    my $job_trig_up = $object->getTrigUp($job_id);
    my $job_dep_down = $object->getDepDown($job_id);
    my $job_trig_down = $object->getTrigDown($job_id);


    my $relation_ref = [];    #������������ϵ����Ϊ�б�ÿ�е�һλΪ��ϵ���ƣ��ڶ�λΪ��ϵref. ���ں�����ѯ����
    my $rela_type = [];
    push @$rela_type, "UP������ҵ:", $job_trig_up if ( $job_trig_up );
    push @$relation_ref, $rela_type if ( $job_trig_up );
    $rela_type = [];
    push @$rela_type, "UP������ҵ:", $job_dep_up if ( $job_dep_up );
    push @$relation_ref, $rela_type if ( $job_dep_up );
    $rela_type = [];
    push @$rela_type, "DOWN������ҵ:", $job_trig_down if ( $job_trig_down );
    push @$relation_ref, $rela_type if ( $job_trig_down );
    $rela_type = [];
    push @$rela_type, "DOWN������ҵ:", $job_dep_down if ( $job_dep_down );
    push @$relation_ref, $rela_type if ( $job_dep_down );
    undef $rela_type;

    my $rela_detail = [];    #��������������ϵ�����
    
#    if ( my $file_nm = $object->getBaseInfo("JOB", $job_id, "08_FILE_NM") )
#    {
#        my $str = [];
#        push @$str, "�ļ�����:";
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
    ## B. ��ӡ��ҵ����������ϵ��
    my $whole = $object->createSegment2("[Relation]", $whole_rela, $sF);
    foreach my $line ( @$whole )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    }

    ## ��ͣ�Ƿ��������
    print "continue(n) show job history log or break(any other key):";
    my $ch = <STDIN>;
    chomp $ch;
    next unless ( uc($ch) eq "N" );
    
    ## ��ҵ��ʷ��־ �� 
    my $para = {};
    $para->{'JOB_NAME'} = $jobName;
    my $job_his = $object->queryJob("JOBHIS", $para);
        
    my $his_window = 30;  # ÿ�����������������־
    my $his_all = scalar(@$job_his);

    my $step = 0;
    while(1)
    {
        my $pop = [];
        @$pop = @$job_his[$step..$step+$his_window];  # �������б��Ƭ�������ڴ�Сȡ
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

    ## 1.���������ȡ����Ϣ
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

    my $sF = spaceFormat->new();   #�ո��ʽ
    my $tF = upcorFormat->new();   #�Ϲҽ�
    my $bF = downcorFormat->new(); #�¹ҽ�
    my $rF = rowFormat->new();     #���и�ʽ
   # $sF->decorate($object);        #��ʼ�ո��ʽ

    ## ��������ӡ��������Ϣ��ʽ��
    my $dis_flow_basic = $object->createSegment2("[Flow Description]", $flow_block, $sF);   #Ϊ��������������ӿո��ʽ
    foreach my $line ( @$dis_flow_basic )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    }

    ## 2.��������ҵ���ڷ���ͳ��
    my $job_list = [ keys %{$object->{'JOB'}} ];  #ȫ����ҵid

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

    my $dis_flow_txdate = $object->createSegment2("[Txdate]", $jobgrp_block, $sF);   #Ϊ��������������ӿո��ʽ
    foreach my $line ( @$dis_flow_txdate )
    {
        ( $_ =~ /\s+$/ )? print $_ : print $_."  " foreach ( @$line );
        print "\n";
    } 

    ### 3.��ҵ���
    #my $flow_dt = $object->getBaseInfo("FLOW", $flow_id, "04_FLOW_DATE");
    #my $updt_job = $object->getBaseInfo("FLOW", $flow_id, "05_UPDT_JOB");
    #my $updt_jobid = $object->getJobId($updt_job);

    # ��ȡ��ĩβ��ҵ
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

        # ��������ҵ�������¼���ҵ
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
    # ����ִ�ж�����Ϣ��
    my $dis_run_block = $object->createSegment2("[Run Queue]", $run_block);

    ## �����ȴ�������ҵ��Ϣ
    my $wait_block = [];
    foreach my $wait_id ( @$wait_queue )
    {
        my $str = [];
        my $wait_snap = $object->getBaseInfo("JOBSNAP", $wait_id);

        if ( my $source_file = $object->getBaseInfo("JOB", $wait_id, "08_FILE_NM") )
        {
            # �ļ�����
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
