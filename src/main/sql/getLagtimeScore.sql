CREATE  FUNCTION `getLagtimeScore`(userid varchar(20)) RETURNS bigint(20)
BEGIN
    # This function compute the average lagtime value for a given user.
    # This lagtime value is based on hashtags given in table "hashtags_analyzed".
    # Creator: Reda Bouadjenek
    DECLARE lag bigint default 0;
    select avg(lagtime) into lag from(
    select hashtag,COALESCE(lagtime,'11100000000') as lagtime from(
    select ha.hashtag, hbpu.birthday-ha.birthday as lagtime
    from hashtags_analyzed ha
    LEFT JOIN `hashtagbirthdayperuser` hbpu
    on ha.hashtag=hbpu.hashtag
    and hbpu.userid=userid) as t1) as t2;
RETURN lag;
END