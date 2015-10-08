CREATE  PROCEDURE `execute_eval2.0`(nbr_users int,`start` int,`stop` int)
BEGIN
    # This stored procedure will create the control group and the sensors groups 
    # by calling the buildGroupAB procedure. Each group contains "nbr_users" users.
    # For each group it will collect its hashtags and store them in a table called "groups_hashtags".
    # It will also generate the hashtags for the groups C, C_bis, C20, and D.
    # This procedure will also fill the h_analysis table.
    # This procedure will iterate from  "start" to "stop".
    # Creator: Reda Bouadjenek
    #delete from analysis;
    #delete from NM;
    set @k=`start`;
    while @k < `stop` do    
        SET @k = @k + 1;
        #*******************************
        #**** INITIALIZATION************
        #*******************************
        Set @N=0;
        set @M1=0;
        set @M2=0;
        set @M3=0;
        set @M4=0;
        drop table if exists tmp_groupsAB;  # store the users control and sensor groups
        drop table if exists tmp_hashtagsA; # store the hashtags of the control group A
        drop table if exists tmp_hashtagsB1; # store the hashtags of the sensor group B1
        drop table if exists tmp_hashtagsB2; # store the hashtags of the sensor group B2
        drop table if exists tmp_hashtagsB3; # store the hashtags of the sensor group B3
        drop table if exists tmp_hashtagsB4; # store the hashtags of the sensor group B4
        drop table if exists tmp_tweetsC20;    # store the tweets of the group C (M random tweets)
        drop table if exists tmp_hashtagsC_20; # store the hashtags of the group C
        drop table if exists tmp_tweetsC;    # store the tweets of the group C (M random tweets)
        drop table if exists tmp_hashtagsC; # store the hashtags of the group C
        drop table if exists tmp_tweetsC_bis;    # store the tweets of the group C (M random tweets)
        drop table if exists tmp_hashtagsC_bis; # store the hashtags of the group C
        drop table if exists tmp_tweetsD; # store the tweets of the group D (N random tweets of the group B)
        drop table if exists tmp_hashtagsD; # store the hashtags of the group D      
        set @h_before_A=0;  # store the total number of hashtags in the group A
        set @h_before_B1=0;  # store the total number of hashtags in the group B1
        set @h_before_B2=0;  # store the total number of hashtags in the group B2
        set @h_before_B3=0;  # store the total number of hashtags in the group B3
        set @h_before_B4=0;  # store the total number of hashtags in the group B4
        set @h_before_C=0;  # store the total number of hashtags in the group C
        set @h_before_C20=0;  # store the total number of hashtags in the group C
        set @h_before_D=0;  # store the total number of hashtags in the group D
        set @h_after_A=0; # store the total number of unique hashtags in the group A
        set @h_after_B1=0; # store the total number of unique hashtags in the group B1
        set @h_after_B2=0; # store the total number of unique hashtags in the group B2
        set @h_after_B3=0; # store the total number of unique hashtags in the group B3
        set @h_after_B4=0; # store the total number of unique hashtags in the group B4
        set @h_after_C=0; # store the total number of unique hashtags in the group C
        set @h_after_C20=0; # store the total number of unique hashtags in the group C
        set @h_after_D=0; # store the total number of unique hashtags in the group D
        #***************************************************************
        #**** SELECTION OF THE SET of user groups controls & sensor*****
        #***************************************************************
        call buildGroupAB('tmp_groupsAB',nbr_users,0);
        insert into groupsAB (
            select  @k as iteration,userid_control,userid_sensor1,userid_sensor2,userid_sensor3,userid_sensor4 
            from tmp_groupsAB);
        #*******************************
        #**** Computing N and M ******
        #*******************************
        select count(*) into @N 
        from  tmp_groupsAB g, tweets t 
        where t.userid=g.userid_control;
        select count(*) into @M1 
        from  tmp_groupsAB g, tweets t 
        where t.userid=g.userid_sensor1;
        select count(*) into @M2 
        from  tmp_groupsAB g, tweets t 
        where t.userid=g.userid_sensor2;
        select count(*) into @M3 
        from  tmp_groupsAB g, tweets t 
        where t.userid=g.userid_sensor3;
        select count(*) into @M4 
        from  tmp_groupsAB g, tweets t 
        where t.userid=g.userid_sensor4;        
        insert into h_analysis (`iteration`, `N`, `M1`, `M2`, `M3`, `M4`, `type`)
         values(@k,@N,@M1,@M2,@M3,@M4,'before');
        insert into h_analysis (`iteration`, `N`, `M1`, `M2`, `M3`, `M4`, `type`)
         values(@k,@N,@M1,@M2,@M3,@M4,'after');
        #******************************************
        #**** Collecting Hashtags of group A ******
        #******************************* **********   
        create TEMPORARY table tmp_hashtagsA ENGINE=MEMORY  as(
        select hashtag,min(birthday) as birthday,sum(hbpu.nbr_tweets) as total
        from tmp_groupsAB g,hashtagbirthdayperuser hbpu
        where g.userid_control=hbpu.userid
        group by hashtag);
        ALTER TABLE tmp_hashtagsA 
        ADD INDEX `index` USING HASH (`hashtag` ASC)  COMMENT '';
        select sum(total) into @h_before_A
        from tmp_hashtagsA;
        select count(*) into @h_after_A 
        from tmp_hashtagsA;
        #******************************************
        #*********** Computing Final results A ******
        #******************************* ********** 
        insert into groups_hashtags (select @k as iteration,'A' as `group`,hashtag,birthday,total from tmp_hashtagsA);
        update h_analysis ha  set ha.h_A=@h_before_A where  ha.iteration=@k and ha.`type`='before' ;
        update h_analysis ha  set ha.h_A=@h_after_A where  ha.iteration=@k and ha.`type`='after' ;
        #******************************************
        #**** Collecting Hashtags of group B1 ******
        #******************************* **********  
        create TEMPORARY table tmp_hashtagsB1 ENGINE=MEMORY  as(
        select hashtag,min(birthday) as birthday,sum(hbpu.nbr_tweets) as total
        from tmp_groupsAB g,hashtagbirthdayperuser hbpu
        where g.userid_sensor1=hbpu.userid
        group by hashtag);
        ALTER TABLE tmp_hashtagsB1 
        ADD INDEX `index` USING HASH (`hashtag` ASC)  COMMENT '';
        select sum(total) into @h_before_B1
        from tmp_hashtagsB1;
        select count(*) into @h_after_B1
        from tmp_hashtagsB1;
        #******************************************
        #*********** Computing Final results B1 ******
        #******************************* ********** 
        insert into groups_hashtags (select @k as iteration,'B1' as `group`,hashtag,birthday,total from tmp_hashtagsB1);
        update h_analysis ha  set ha.h_B1=@h_before_B1 where  ha.iteration=@k and ha.`type`='before' ;
        update h_analysis ha  set ha.h_B1=@h_after_B1 where  ha.iteration=@k and ha.`type`='after' ;
        #******************************************
        #**** Collecting Hashtags of group B2 ******
        #******************************* **********  
        create TEMPORARY table tmp_hashtagsB2 ENGINE=MEMORY  as(
        select hashtag,min(birthday) as birthday,sum(hbpu.nbr_tweets) as total
        from tmp_groupsAB g,hashtagbirthdayperuser hbpu
        where g.userid_sensor2=hbpu.userid
        group by hashtag);
        ALTER TABLE tmp_hashtagsB2 
        ADD INDEX `index` USING HASH (`hashtag` ASC)  COMMENT '';
        select sum(total) into @h_before_B2
        from tmp_hashtagsB2;
        select count(*) into @h_after_B2
        from tmp_hashtagsB2;
        #******************************************
        #*********** Computing Final results B2 ******
        #******************************* ********** 
        insert into groups_hashtags (select @k as iteration,'B2' as `group`,hashtag,birthday,total from tmp_hashtagsB2);
        update h_analysis ha  set ha.h_B2=@h_before_B2 where  ha.iteration=@k and ha.`type`='before' ;
        update h_analysis ha  set ha.h_B2=@h_after_B2 where  ha.iteration=@k and ha.`type`='after' ;
        #******************************************
        #**** Collecting Hashtags of group B3 ******
        #******************************* **********  
        create TEMPORARY table tmp_hashtagsB3 ENGINE=MEMORY  as(
        select hashtag,min(birthday) as birthday,sum(hbpu.nbr_tweets) as total
        from tmp_groupsAB g,hashtagbirthdayperuser hbpu
        where g.userid_sensor3=hbpu.userid
        group by hashtag);
        ALTER TABLE tmp_hashtagsB3 
        ADD INDEX `index` USING HASH (`hashtag` ASC)  COMMENT '';
        select sum(total) into @h_before_B3
        from tmp_hashtagsB3;
        select count(*) into @h_after_B3
        from tmp_hashtagsB3;
        #******************************************
        #*********** Computing Final results B3 ******
        #******************************* ********** 
        insert into groups_hashtags (select @k as iteration,'B3' as `group`,hashtag,birthday,total from tmp_hashtagsB3);
        update h_analysis ha  set ha.h_B3=@h_before_B3 where  ha.iteration=@k and ha.`type`='before' ;
        update h_analysis ha  set ha.h_B3=@h_after_B3 where  ha.iteration=@k and ha.`type`='after' ;
        #******************************************
        #**** Collecting Hashtags of group B4 ******
        #******************************* **********  
        create TEMPORARY table tmp_hashtagsB4 ENGINE=MEMORY  as(
        select hashtag,min(birthday) as birthday,sum(hbpu.nbr_tweets) as total
        from tmp_groupsAB g,hashtagbirthdayperuser hbpu
        where g.userid_sensor4=hbpu.userid
        group by hashtag);
        ALTER TABLE tmp_hashtagsB4 
        ADD INDEX `index` USING HASH (`hashtag` ASC)  COMMENT '';
        select sum(total) into @h_before_B4
        from tmp_hashtagsB4;
        select count(*) into @h_after_B4
        from tmp_hashtagsB4;
        #******************************************
        #*********** Computing Final results B4******
        #******************************* **********
        insert into groups_hashtags (select @k as iteration,'B4' as `group`,hashtag,birthday,total from tmp_hashtagsB4); 
        update h_analysis ha  set ha.h_B4=@h_before_B4 where  ha.iteration=@k and ha.`type`='before' ;
        update h_analysis ha  set ha.h_B4=@h_after_B4 where  ha.iteration=@k and ha.`type`='after' ;
        #******************************************
        #**** Collecting Tweets of group C ******
        #******************************* **********
        call buildTweetsTable('tmp_tweetsC', @M1, 0);        
        #******************************************
        #**** Collecting Hashtags of group C ******
        #******************************* ********** 
        create TEMPORARY table tmp_hashtagsC ENGINE=MEMORY  as(
        select th.hashtag,min(th.`date`) as birthday,count(*) as total
        from tmp_tweetsC t,tweets_hashtags th
        where t.tweetid=th.tweetid
        group by th.hashtag);
        ALTER TABLE tmp_hashtagsC         
        ADD INDEX `index` USING HASH (`hashtag` ASC)  COMMENT '';
        select sum(total) into @h_before_C
        from tmp_hashtagsC;
        select count(*) into @h_after_C 
        from tmp_hashtagsC;  
        #******************************************
        #*********** Computing Final results C ******
        #******************************* **********
        insert into groups_hashtags (select @k as iteration,'C' as `group`,hashtag,birthday,total from tmp_hashtagsC); 
        update h_analysis ha  set ha.h_C=@h_before_C where  ha.iteration=@k and ha.`type`='before' ;
        update h_analysis ha  set ha.h_C=@h_after_C where  ha.iteration=@k and ha.`type`='after' ;
        #******************************************
        #**** Collecting Tweets of group C_bis ******
        #******************************* **********
        call buildTweetsTable('tmp_tweetsC_bis', @M4, 0);        
        #******************************************
        #**** Collecting Hashtags of group C_bis ******
        #******************************* ********** 
        create TEMPORARY table tmp_hashtagsC_bis ENGINE=MEMORY  as(
        select th.hashtag,min(th.`date`) as birthday,count(*) as total
        from tmp_tweetsC_bis t,tweets_hashtags th
        where t.tweetid=th.tweetid
        group by th.hashtag);
        ALTER TABLE tmp_hashtagsC_bis         
        ADD INDEX `index` USING HASH (`hashtag` ASC)  COMMENT '';
        select sum(total) into @h_before_C_bis
        from tmp_hashtagsC_bis;
        select count(*) into @h_after_C_bis 
        from tmp_hashtagsC_bis;
        #******************************************
        #*********** Computing Final results C_bis ******
        #******************************* **********
        insert into groups_hashtags (select @k as iteration,'C_bis' as `group`,hashtag,birthday,total from tmp_hashtagsC_bis); 
        update h_analysis ha  set ha.h_C_bis=@h_before_C_bis where  ha.iteration=@k and ha.`type`='before' ;
        update h_analysis ha  set ha.h_C_bis=@h_after_C_bis where  ha.iteration=@k and ha.`type`='after' ;
        #******************************************
        #**** Collecting Tweets of group C20 ******
        #******************************* **********
        SET @sql := CONCAT('CREATE TEMPORARY TABLE tmp_tweetsC20 ENGINE=MEMORY as ( SELECT * FROM tweets_usersh20 order by rand() LIMIT ', @M1, ')');
        PREPARE stmt1 FROM @sql;
        EXECUTE stmt1;
        ALTER TABLE tmp_tweetsC20 
        ADD INDEX `index` USING BTREE (`tweetid` ASC)  COMMENT '';
        #******************************************
        #**** Collecting Hashtags of group C20 ******
        #******************************* ********** 
        create TEMPORARY table tmp_hashtagsC_20 ENGINE=MEMORY  as(
        select th.hashtag,min(th.`date`) as birthday,count(*) as total
        from tmp_tweetsC20 t,tweets_hashtags th
        where t.tweetid=th.tweetid
        group by th.hashtag);
        ALTER TABLE tmp_hashtagsC_20         
        ADD INDEX `index` USING HASH (`hashtag` ASC)  COMMENT '';
        select sum(total) into @h_before_C20
        from tmp_hashtagsC_20;
        select count(*) into @h_after_C20 
        from tmp_hashtagsC_20; 
        #******************************************
        #*********** Computing Final results C20 ******
        #******************************* **********
        insert into groups_hashtags (select @k as iteration,'C20' as `group`,hashtag,birthday,total from tmp_hashtagsC_20);  
        update h_analysis ha  set ha.h_C20=@h_before_C20 where  ha.iteration=@k and ha.`type`='before' ;
        update h_analysis ha  set ha.h_C20=@h_after_C20 where  ha.iteration=@k and ha.`type`='after' ;    
        #******************************************
        #**** Collecting tweets of group D ******
        #******************************* **********         
        SET @sql := CONCAT('create TEMPORARY table tmp_tweetsD as(SELECT tweetid FROM tmp_groupsAB g,tweets t where g.userid_sensor1=t.userid order by rand() limit ', @N, ')');
        PREPARE stmt1 FROM @sql;
        EXECUTE stmt1;   
        ALTER TABLE tmp_tweetsD 
        ADD INDEX `index` USING HASH (`tweetid` ASC)  COMMENT '';
        #******************************************
        #**** Collecting Hashtags of group D ******
        #******************************* ********** 
        create TEMPORARY table tmp_hashtagsD ENGINE=MEMORY as(
        select tk.hashtag,min(birthday) as birthday,count(*) as total
        from tmp_tweetsD t,tweets_hashtags tk,hashtagbirthdayperuser hbpu
        where t.tweetid=tk.tweetid and tk.userid=hbpu.userid and tk.hashtag=hbpu.hashtag
        group by tk.hashtag);
        ALTER TABLE tmp_hashtagsD
        ADD INDEX `index` USING HASH (`hashtag` ASC)  COMMENT '';
        select sum(total) into @h_before_D
        from tmp_hashtagsD;
        select count(*) into @h_after_D 
        from tmp_hashtagsD; 
        #******************************************
        #*********** Computing Final results D ******
        #******************************* ********** 
        insert into groups_hashtags (select @k as iteration,'D' as `group`,hashtag,birthday,total from tmp_hashtagsD);  
        update h_analysis ha  set ha.h_D=@h_before_D where  ha.iteration=@k and ha.`type`='before' ;
        update h_analysis ha  set ha.h_D=@h_after_D where  ha.iteration=@k and ha.`type`='after' ;  
        #******************************************
        #******************************************
        select CONCAT('Iteration number ',@k,' is completed.') as 'Message';
        end while;
END