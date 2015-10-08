CREATE FUNCTION `getRandomMention`(uid varchar(20),nbr_hashtags int) RETURNS varchar(20) CHARSET utf8
BEGIN
    # This function randomly select for a given user, a user he mentionned and who used at least nbr_hashtags hashtags.
    # Creator: Reda Bouadjenek
    DECLARE userid_s varchar(20) default null;
    select userid2  into userid_s
    from `mentionnetwork` mn,users u2
    where mn.userid1=uid and 
    mn.userid2=u2.userid and 
    u2.nbr_hashtags>=nbr_hashtags and
    userid1!=userid2 
    order by rand()
    limit 1;
RETURN userid_s;
END