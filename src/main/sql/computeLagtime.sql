CREATE  PROCEDURE `computeLagtime`(hashtagsTable varchar(45),results varchar(45))
BEGIN
    # This stored procedure will compute the lagtime for each group, each hashtag, and each iteration.
    # The list of hashtags used to compute the lagtime is given in the table "hashtagsTable".
    # The resuls (the cube) are stored in a table called "results".
    # Creator: Reda Bouadjenek
    declare done INT default false;
    declare i,iter INT(11);
    declare g varchar(45);
    declare cur1 cursor FOR  select iteration,`group` 
    from groups_hashtags
    where iteration <=20
    group by iteration,`group` ;
    declare continue handler for not found set done = true;
    drop table if exists tmp;
    create TEMPORARY table tmp ENGINE=MEMORY  as(
    select iteration from groups_hashtags  group by iteration);
    SET @s := CONCAT('delete from ',results,';');
    PREPARE stmt1 FROM @s;
    EXECUTE stmt1; 
    SET @s := CONCAT('insert into ',results,' (iteration,hashtag)(
        select iteration,hashtag 
        from tmp,',hashtagsTable,' 
        order by iteration,hashtag);');
    PREPARE stmt1 FROM @s;
    EXECUTE stmt1; 
    drop table if exists tmp;
    #*******************************
    set i=0; # initialize the counter   
    #*******************************
    open cur1;
    read_loop: loop
        fetch cur1 INTO iter,g;
        if done then
            leave read_loop;
        end if;
        set i=i+1;
        #*******************************
        #**** initialization*****
        #*******************************
        drop table if exists tmp1;
        create TEMPORARY table tmp1 ENGINE=MEMORY  as(
            select hashtag,birthday from groups_hashtags  
            where iteration=iter and `group`=g
            );
        ALTER TABLE tmp1 
        ADD INDEX `index` USING HASH (`hashtag` ASC)  COMMENT '';
        drop table if exists tmp2;
        SET @s := CONCAT('create TEMPORARY table tmp2 as(
        select hashtag,COALESCE(lagtime,''58060799000'') as lagtime from(
        select ha.hashtag, UNIX_TIMESTAMP(tmp1.birthday)-UNIX_TIMESTAMP(ha.birthday) as lagtime
        from ',hashtagsTable,' ha
        LEFT JOIN tmp1 
        on ha.hashtag=tmp1.hashtag) as t1);');
        PREPARE stmt1 FROM @s;
        EXECUTE stmt1; 
        ALTER TABLE tmp2 
        ADD INDEX `index` USING BTREE (`hashtag` ASC)  COMMENT '';
        #******************************************
        #*********** computing final results ******
        #******************************* ********** 
        SET @s := CONCAT('update ',results,' a,tmp2  
            set a.lag_time',g,'=tmp2.lagtime 
            where  a.iteration=',iter,' and a.hashtag=tmp2.hashtag ;');
        PREPARE stmt1 FROM @s;
        EXECUTE stmt1;   
        select CONCAT('Lagtime computed for iteration: ',iter,' and group: ',g,'.') as 'Message';
    END LOOP;
    close cur1;
END