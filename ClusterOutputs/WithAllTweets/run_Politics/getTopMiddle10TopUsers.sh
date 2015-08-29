#totalCEFromUser: 95547199
#totalCEToUser: 49609385
#totalCEFromHashtag: 11183410

#Hashtag: 5591700, 5591709p
#FromUser: 47773594, 47773603p
#ToUser: 24804687, 24804696p

#Hashtag: 11183401, 11183410p
#FromUser: 95547190, 95547199p
#ToUser: 49609376, 49609385p

echo top10_CEFromHashtag
sed -n '1,10p' CSVOut_CondEntropyTweetFromHashtag_2_parquet.csv > top10_CSVOut_CondEntropyTweetContainHashtag.csv
echo top10_CEToUser
sed -n '1,10p' CSVOut_CondEntropyTweetToUser_2_parquet.csv > top10_CSVOut_CondEntropyTweetToUser.csv
echo top10_CEFromUser
sed -n '1,10p' CSVOut_CondEntropyTweetFromUser_2_parquet.csv > top10_CSVOut_CondEntropyTweetFromUser.csv
echo top10_MIFromHashtag
sed -n '1,10p' CSVOut_mutualEntropyTweetFromHashtag_2_parquet.csv > top10_CSVOut_mutualEntropyTweetContainHashtag.csv
echo top10_MIFromUser
sed -n '1,10p' CSVOut_mutualEntropyTweetFromUser_2_parquet.csv > top10_CSVOut_mutualEntropyTweetFromUser.csv
echo top10_MIToUser
sed -n '1,10p' CSVOut_mutualEntropyTweetToUser_2_parquet.csv > top10_CSVOut_mutualEntropyTweetToUser.csv
echo "top10_MIP(Contain=T|ContainHashtag=T)"
sed -n '1,10p' CSVOut_ProbTweetTrueContainHashtagTrue_2_parquet.csv > top10_ProbTweetTrueContainHashtagTrue.csv
echo "top10_MIP(Contain=T|FromUser=T)"
sed -n '1,10p' CSVOut_ProbTweetTrueFromUserTrue_2_parquet.csv > top10_ProbTweetTrueFromUserTrue.csv
echo "top10_MIP(Contain=T|ToUser=T)"
sed -n '1,10p' CSVOut_ProbTweetTrueToUserTrue_2_parquet.csv > top10_ProbTweetTrueToUserTrue.csv

echo middle10_CEFromHashtag
sed -n '5591700, 5591709p' CSVOut_CondEntropyTweetFromHashtag_2_parquet.csv >  middle10_CSVOut_CondEntropyTweetContainHashtag.csv
echo middle10_CEToUser
sed -n '24804687, 24804696p' CSVOut_CondEntropyTweetToUser_2_parquet.csv > middle10_CSVOut_CondEntropyTweetToUser.csv
echo middle10_CEFromUser
sed -n '47773594, 47773603p' CSVOut_CondEntropyTweetFromUser_2_parquet.csv > middle10_CSVOut_CondEntropyTweetFromUser.csv
echo middle10_MIFromHashtag
sed -n '5591700, 5591709p' CSVOut_mutualEntropyTweetFromHashtag_2_parquet.csv > middle10_CSVOut_mutualEntropyTweetContainHashtag.csv
echo middle10_MIFromUser
sed -n '47773594, 47773603p' CSVOut_mutualEntropyTweetFromUser_2_parquet.csv > middle10_CSVOut_mutualEntropyTweetFromUser.csv
echo middle10_MIToUser
sed -n '24804687, 24804696p' CSVOut_mutualEntropyTweetToUser_2_parquet.csv > middle10_CSVOut_mutualEntropyTweetToUser.csv
echo "middle10_MIP(Contain=T|ContainHashtag=T)"
sed -n '5591700, 5591709p' CSVOut_ProbTweetTrueContainHashtagTrue_2_parquet.csv > middle10_ProbTweetTrueContainHashtagTrue.csv
echo "middle10_MIP(Contain=T|FromUser=T)"
sed -n '47773594, 47773603p' CSVOut_ProbTweetTrueFromUserTrue_2_parquet.csv > middle10_ProbTweetTrueFromUserTrue.csv
echo "middle10_MIP(Contain=T|ToUser=T)"
sed -n '24804687, 24804696p' CSVOut_ProbTweetTrueToUserTrue_2_parquet.csv > middle10_ProbTweetTrueToUserTrue.csv

echo tail10_CEFromHashtag
sed -n '11183401,11183410p' CSVOut_CondEntropyTweetFromHashtag_2_parquet.csv > tail10_CSVOut_CondEntropyTweetContainHashtag.csv
echo tail10_CEToUser
sed -n '49609376, 49609385p' CSVOut_CondEntropyTweetToUser_2_parquet.csv > tail10_CSVOut_CondEntropyTweetToUser.csv
echo tail10_CEFromUser
sed -n '95547190, 95547199p' CSVOut_CondEntropyTweetFromUser_2_parquet.csv > tail10_CSVOut_CondEntropyTweetFromUser.csv
echo tail10_MIFromHashtag
sed -n '11183401,11183410p' CSVOut_mutualEntropyTweetFromHashtag_2_parquet.csv > tail10_CSVOut_mutualEntropyTweetContainHashtag.csv
echo tail10_MIFromUser
sed -n '95547190, 95547199p' CSVOut_mutualEntropyTweetFromUser_2_parquet.csv > tail10_CSVOut_mutualEntropyTweetFromUser.csv
echo tail10_MIToUser
sed -n '49609376, 49609385p' CSVOut_mutualEntropyTweetToUser_2_parquet.csv > tail10_CSVOut_mutualEntropyTweetToUser.csv
echo "tail10_MIP(Contain=T|ContainHashtag=T)"
sed -n '11183401,11183410p' CSVOut_ProbTweetTrueContainHashtagTrue_2_parquet.csv > tail10_ProbTweetTrueContainHashtagTrue.csv
echo "tail10_MIP(Contain=T|FromUser=T)"
sed -n '95547190, 95547199p' CSVOut_ProbTweetTrueFromUserTrue_2_parquet.csv > tail10_ProbTweetTrueFromUserTrue.csv
echo "tail10_MIP(Contain=T|ToUser=T)"
sed -n '49609376, 49609385p' CSVOut_ProbTweetTrueToUserTrue_2_parquet.csv > tail10_ProbTweetTrueToUserTrue.csv

echo Non_Zero_Results
echo middle10_CEFromHashtag
sed -n '9274, 9283p' NoZero_CSVOut_CondEntropyTweetFromHashtag_2_parquet.csv >  middle10_NonZero_CSVOut_CondEntropyTweetContainHashtag.csv
echo middle10_CEToUser
sed -n '23848, 23857p' NoZero_CSVOut_CondEntropyTweetToUser_2_parquet.csv > middle10_NonZero_CSVOut_CondEntropyTweetToUser.csv
echo middle10_CEFromUser
sed -n '71904, 71913p' NoZero_CSVOut_CondEntropyTweetFromUser_2_parquet.csv > middle10_NonZero_CSVOut_CondEntropyTweetFromUser.csv
echo "middle10_MIP(Contain=T|ContainHashtag=T)"
sed -n '11319, 11328p' NoZero_CSVOut_ProbTweetTrueContainHashtagTrue_2_parquet.csv > middle10_NonZero_ProbTweetTrueContainHashtagTrue.csv
echo "middle10_MIP(Contain=T|FromUser=T)"
sed -n '78511, 78520p' NoZero_CSVOut_ProbTweetTrueFromUserTrue_2_parquet.csv > middle10_NonZero_ProbTweetTrueFromUserTrue.csv
echo "middle10_MIP(Contain=T|ToUser=T)"
sed -n '25721, 25730p' NoZero_CSVOut_ProbTweetTrueToUserTrue_2_parquet.csv > middle10_NonZero_ProbTweetTrueToUserTrue.csv

echo tail10_CEFromHashtag
sed -n '18549, 18558p' NoZero_CSVOut_CondEntropyTweetFromHashtag_2_parquet.csv > tail10_NonZero_CSVOut_CondEntropyTweetContainHashtag.csv
echo tail10_CEToUser
sed -n '47697, 47706p' NoZero_CSVOut_CondEntropyTweetToUser_2_parquet.csv > tail10_NonZero_CSVOut_CondEntropyTweetToUser.csv
echo tail10_CEFromUser
sed -n '143810, 143819p' NoZero_CSVOut_CondEntropyTweetFromUser_2_parquet.csv > tail10_NonZero_CSVOut_CondEntropyTweetFromUser.csv
echo "tail10_MIP(Contain=T|ContainHashtag=T)"
sed -n '22639, 22648p' NoZero_CSVOut_ProbTweetTrueContainHashtagTrue_2_parquet.csv > tail10_NonZero_ProbTweetTrueContainHashtagTrue.csv
echo "tail10_MIP(Contain=T|FromUser=T)"
sed -n '157024, 157033p' NoZero_CSVOut_ProbTweetTrueFromUserTrue_2_parquet.csv > tail10_NonZero_ProbTweetTrueFromUserTrue.csv
echo "tail10_MIP(Contain=T|ToUser=T)"
sed -n '51444, 51453p' NoZero_CSVOut_ProbTweetTrueToUserTrue_2_parquet.csv > tail10_NonZero_ProbTweetTrueToUserTrue.csv



