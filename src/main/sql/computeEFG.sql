CREATE  PROCEDURE `computeEFG`(`start` int,`stop` int)
BEGIN
    # This stored procedure will generate the hashtags for the groups E,F, and G.
    # The lists of hashtags is stored in a table called "groups_hashtags"
    # E: M1 tweets of users with top lagtime value
    # F: M1 tweets of users who used the most hashtags
    # G: M1 tweets of most active users (users with top nbr_hashtag value)
    # Creator: Reda Bouadjenek

    declare done INT default false;
    declare i,iter,M_1 INT(11);
    declare cur1 cursor FOR  select iteration,M1 
    from h_analysis 
    where iteration > `start` and iteration < `stop`  and h_G is null
    group by iteration ,M1;
    declare continue handler for not found set done = true;
    drop table if exists topUsers;
    drop table if exists tmp_tweetsE1;
    create temporary table topUsers engine=memory  as(
            select userid 
            from users 
            order by lagtime 
            limit 1000
            );
        alter table topUsers         
        add index `index` using hash (`userid` asc)  comment '';
    create temporary table  tmp_tweetsE1 engine=memory as (
        select t.tweetid
        from tweets t,topUsers top
        where t.userid =top.userid);

    drop table if exists topUsers;
    drop table if exists tmp_tweetsF1;
    create temporary table topUsers engine=memory  as(
            select * 
            from users 
            order by nbr_hashtags desc 
            limit 1000
            );
    alter table topUsers         
        add index `index` using hash (`userid` asc)  comment '';
    create temporary table  tmp_tweetsF1 engine=memory as (
        select t.tweetid
        from tweets t,topUsers top
        where t.userid =top.userid);
    drop table if exists topUsers;
    create temporary table topUsers engine=memory  as(
                select userid 
                from users 
                order by nbr_tweets desc
                limit 1000
                );
        alter table topUsers         
        add index `index` using hash (`userid` asc)  comment '';
        drop table if exists tmp_tweetsG1;   
        create temporary table  tmp_tweetsG1 engine=memory as (
            select t.tweetid
            from tweets t,topUsers top
            where t.userid =top.userid);
        drop table if exists topUsers;
    #*******************************
    set i=0; # initialize the counter   
    #*******************************
    open cur1;
    read_loop: loop
        fetch cur1 INTO iter,M_1;
        if done then
            leave read_loop;
        end if;
        set i=i+1;
        #*******************************
        #**** initialization*****
        #*******************************
        drop table if exists E; # store the lag-time of the group E
        drop table if exists tmp_tweetsE2;    # store the tweets of the group E (M random tweets)
        drop table if exists tmp_hashtagsE; # store the hashtags of the group E
        set @h_before_E=0;  # store the total number of hashtags in the group E
        set @h_after_E=0; # store the total number of unique hashtags in the group E
        #******************************************
        #**** collecting tweets of group E ******
        #******************************* **********
        set @s := concat('create temporary table  tmp_tweetsE2 engine=memory as (
        select *
        from tmp_tweetsE1
        order by rand()
        limit ',M_1,');');
        prepare stmt1 FROM @s;
        execute stmt1;
        alter table tmp_tweetsE2 
        add index `index` using hash (`tweetid` ASC)  comment '';     
        #******************************************
        #**** collecting hashtags of group E ******
        #******************************* ********** 
        create temporary table tmp_hashtagsE engine=memory  as(
        select th.hashtag,min(th.`date`) as birthday,count(*) as total
        from tmp_tweetsE2 t,tweets_hashtags th
        where t.tweetid=th.tweetid
        group by th.hashtag);
        alter table tmp_hashtagsE         
        add index `index` using hash (`hashtag` asc)  comment '';
        select sum(total) into @h_before_E
        from tmp_hashtagsE;
        select count(*) into @h_after_E 
        from tmp_hashtagsE; 
        #******************************************
        #*********** computing final results ******
        #******************************* ********** 
        insert into groups_hashtags (select iter as iteration,'E' as `group`,hashtag,birthday,total from tmp_hashtagsE);
        update h_analysis ha set ha.h_E=@h_before_E where  ha.iteration=iter and ha.`type`='before' ;
        update h_analysis ha set ha.h_E=@h_after_E where  ha.iteration=iter and ha.`type`='after' ;
        #*******************************
        #**** initialization*****
        #*******************************
        drop table if exists F; # store the lag-time of the group F
        drop table if exists tmp_tweetsF2;    # store the tweets of the group F (M random tweets)
        drop table if exists tmp_hashtagsF; # store the hashtags of the group F
        drop table if exists topUsers;
        set @h_before_F=0;  # store the total number of hashtags in the group F
        set @h_after_F=0; # store the total number of unique hashtags in the group F
        #******************************************
        #**** collecting tweets of group F ******
        #******************************* **********
        set @s := concat('create temporary table  tmp_tweetsF2 engine=memory as (
        select *
        from tmp_tweetsF1
        order by rand()
        limit ',M_1,');');
        prepare stmt1 FROM @s;
        execute stmt1;
        alter table tmp_tweetsF2 
        add index `index` using hash (`tweetid` ASC)  comment '';     
        #******************************************
        #**** collecting hashtags of group F ******
        #******************************* ********** 
        create temporary table tmp_hashtagsF engine=memory  as(
        select th.hashtag,min(th.`date`) as birthday,count(*) as total
        from tmp_tweetsF2 t,tweets_hashtags th
        where t.tweetid=th.tweetid
        group by th.hashtag);
        alter table tmp_hashtagsF         
        add index `index` using hash (`hashtag` asc)  comment '';
        select sum(total) into @h_before_F
        from tmp_hashtagsF;
        select count(*) into @h_after_F 
        from tmp_hashtagsF; 
        #******************************************
        #*********** computing final results ******
        #******************************* ********** 
        insert into groups_hashtags (select iter as iteration,'F' as `group`,hashtag,birthday,total from tmp_hashtagsF);
        update h_analysis ha set ha.h_F=@h_before_F where  ha.iteration=iter and ha.`type`='before' ;
        update h_analysis ha set ha.h_F=@h_after_F where  ha.iteration=iter and ha.`type`='after' ;
        #*******************************
        #**** initialization*****
        #*******************************
        drop table if exists G; # store the lag-time of the group G
        drop table if exists tmp_tweetsG2;    # store the tweets of the group G (M random tweets)
        drop table if exists tmp_hashtagsG; # store the hashtags of the group G
        set @h_before_G=0;  # store the total number of hashtags in the group G
        set @h_after_G=0; # store the total number of unique hashtags in the group G
        #******************************************
        #***** collecting tweets of group G ******
        #******************************* **********     
        set @s := concat('create temporary table  tmp_tweetsG2 engine=memory as (
        select *
        from tmp_tweetsG1
        order by rand()
        limit ',M_1,');');
        prepare stmt1 FROM @s;
        execute stmt1;
        alter table tmp_tweetsG2
        add index `index` using hash (`tweetid` ASC)  comment '';     
        #******************************************
        #**** collecting hashtags of group G ******
        #******************************* ********** 
        create temporary table tmp_hashtagsG engine=memory  as(
        select th.hashtag,min(th.`date`) as birthday,count(*) as total
        from tmp_tweetsG2 t,tweets_hashtags th
        where t.tweetid=th.tweetid
        group by th.hashtag);
        alter table tmp_hashtagsG         
        add index `index` using hash (`hashtag` asc)  comment '';
        select sum(total) into @h_before_G
        from tmp_hashtagsG;
        select count(*) into @h_after_G 
        from tmp_hashtagsG;
        #******************************************
        #*********** computing final results ******
        #******************************* ********** 
        insert into groups_hashtags (select iter as iteration,'G' as `group`,hashtag,birthday,total from tmp_hashtagsG);
        update h_analysis ha set ha.h_G=@h_before_G where  ha.iteration=iter and ha.`type`='before' ;
        update h_analysis ha set ha.h_G=@h_after_G where  ha.iteration=iter and ha.`type`='after' ;
        select CONCAT('Iteration number ',iter,' is completed for groups E, F and G.') as 'Message';
    END LOOP;
    close cur1;
END