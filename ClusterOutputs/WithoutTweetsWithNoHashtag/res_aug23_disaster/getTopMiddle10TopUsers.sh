#totalCEFromUser: 34232482
#totalCEToUser: 13434395
#totalCEFromHashtag: 11183410

#CE_FromUser: 17116236, 17116246
#CE_ToUser: 6717192, 6717202
#CE_FromHashtag: 5591700, 5591710

echo top10_CEFromHashtag
sed -n '1,10p' CSVOut_CondEntropyTweetFromHashtag_parquet.csv > top10_CSVOut_CondEntropyTweetContainHashtag.csv
echo top10_CEToUser
sed -n '1,10p' CSVOut_CondEntropyTweetToUser_parquet.csv > top10_CSVOut_CondEntropyTweetToUser.csv
echo top10_CEFromUser
sed -n '1,10p' CSVOut_CondEntropyTweetFromUser_parquet.csv > top10_CSVOut_CondEntropyTweetFromUser.csv
echo top10_MIFromHashtag
sed -n '1,10p' CSVOut_mutualEntropyTweetFromHashtag_parquet.csv > top10_CSVOut_mutualEntropyTweetContainHashtag.csv
echo top10_MIFromUser
sed -n '1,10p' CSVOut_mutualEntropyTweetFromUser_parquet.csv > top10_CSVOut_mutualEntropyTweetFromUser.csv
echo top10_MIToUser
sed -n '1,10p' CSVOut_mutualEntropyTweetToUser_parquet.csv > top10_CSVOut_mutualEntropyTweetToUser.csv
echo "top10_MIP(Contain=T|ContainHashtag=T)"
sed -n '1,10p' CSVOut_ProbTweetTrueContainHashtagTrue_1_parquet.csv > top10_ProbTweetTrueContainHashtagTrue.csv
echo "top10_MIP(Contain=T|FromUser=T)"
sed -n '1,10p' CSVOut_ProbTweetTrueFromUserTrue_1_parquet.csv > top10_ProbTweetTrueFromUserTrue.csv
echo "top10_MIP(Contain=T|ToUser=T)"
sed -n '1,10p' CSVOut_ProbTweetTrueToUserTrue_1_parquet.csv > top10_ProbTweetTrueToUserTrue.csv

echo middle10_CEFromHashtag
sed -n '5591700, 5591710p' CSVOut_CondEntropyTweetFromHashtag_parquet.csv >  middle10_CSVOut_CondEntropyTweetContainHashtag.csv
echo middle10_CEToUser
sed -n '6717192, 6717202p' CSVOut_CondEntropyTweetToUser_parquet.csv > middle10_CSVOut_CondEntropyTweetToUser.csv
echo middle10_CEFromUser
sed -n '17116236, 17116246p' CSVOut_CondEntropyTweetFromUser_parquet.csv > middle10_CSVOut_CondEntropyTweetFromUser.csv
echo middle10_MIFromHashtag
sed -n '5591700, 5591710p' CSVOut_mutualEntropyTweetFromHashtag_parquet.csv > middle10_CSVOut_mutualEntropyTweetContainHashtag.csv
echo middle10_MIFromUser
sed -n '17116236, 17116246p' CSVOut_mutualEntropyTweetFromUser_parquet.csv > middle10_CSVOut_mutualEntropyTweetFromUser.csv
echo middle10_MIToUser
sed -n '6717192, 6717202p' CSVOut_mutualEntropyTweetToUser_parquet.csv > middle10_CSVOut_mutualEntropyTweetToUser.csv
echo "middle10_MIP(Contain=T|ContainHashtag=T)"
sed -n '5591700, 5591710p' CSVOut_ProbTweetTrueContainHashtagTrue_1_parquet.csv > middle10_ProbTweetTrueContainHashtagTrue.csv
echo "middle10_MIP(Contain=T|FromUser=T)"
sed -n '17116236, 17116246p' CSVOut_ProbTweetTrueFromUserTrue_1_parquet.csv > middle10_ProbTweetTrueFromUserTrue.csv
echo "middle10_MIP(Contain=T|ToUser=T)"
sed -n '6717192, 6717202p' CSVOut_ProbTweetTrueToUserTrue_1_parquet.csv > middle10_ProbTweetTrueToUserTrue.csv

echo tail10_CEFromHashtag
sed -n '11183400,11183410p' CSVOut_CondEntropyTweetFromHashtag_parquet.csv > tail10_CSVOut_CondEntropyTweetContainHashtag.csv
echo tail10_CEToUser
sed -n '13434385,13434395p' CSVOut_CondEntropyTweetToUser_parquet.csv > tail10_CSVOut_CondEntropyTweetToUser.csv
echo tail10_CEFromUser
sed -n '34232472,34232482p' CSVOut_CondEntropyTweetFromUser_parquet.csv > tail10_CSVOut_CondEntropyTweetFromUser.csv
echo tail10_MIFromHashtag
sed -n '11183400,11183410p' CSVOut_mutualEntropyTweetFromHashtag_parquet.csv > tail10_CSVOut_mutualEntropyTweetContainHashtag.csv
echo tail10_MIFromUser
sed -n '34232472,34232482p' CSVOut_mutualEntropyTweetFromUser_parquet.csv > tail10_CSVOut_mutualEntropyTweetFromUser.csv
echo tail10_MIToUser
sed -n '13434385,13434395p' CSVOut_mutualEntropyTweetToUser_parquet.csv > tail10_CSVOut_mutualEntropyTweetToUser.csv
echo "tail10_MIP(Contain=T|ContainHashtag=T)"
sed -n '11183400,11183410p' CSVOut_ProbTweetTrueContainHashtagTrue_1_parquet.csv > tail10_ProbTweetTrueContainHashtagTrue.csv
echo "tail10_MIP(Contain=T|FromUser=T)"
sed -n '34232472,34232482p' CSVOut_ProbTweetTrueFromUserTrue_1_parquet.csv > tail10_ProbTweetTrueFromUserTrue.csv
echo "tail10_MIP(Contain=T|ToUser=T)"
sed -n '13434385,13434395p' CSVOut_ProbTweetTrueToUserTrue_1_parquet.csv > tail10_ProbTweetTrueToUserTrue.csv

echo middle10_CEFromHashtag
sed -n '5591700, 5591710p' CSVOut_CondEntropyTweetFromHashtag_parquet.csv >  middle10_CSVOut_CondEntropyTweetContainHashtag.csv
echo middle10_CEToUser
sed -n '6717192, 6717202p' CSVOut_CondEntropyTweetToUser_parquet.csv > middle10_CSVOut_CondEntropyTweetToUser.csv
echo middle10_CEFromUser
sed -n '17116236, 17116246p' CSVOut_CondEntropyTweetFromUser_parquet.csv > middle10_CSVOut_CondEntropyTweetFromUser.csv
echo middle10_MIFromHashtag
sed -n '5591700, 5591710p' CSVOut_mutualEntropyTweetFromHashtag_parquet.csv > middle10_CSVOut_mutualEntropyTweetContainHashtag.csv
echo middle10_MIFromUser
sed -n '17116236, 17116246p' CSVOut_mutualEntropyTweetFromUser_parquet.csv > middle10_CSVOut_mutualEntropyTweetFromUser.csv
echo middle10_MIToUser
sed -n '6717192, 6717202p' CSVOut_mutualEntropyTweetToUser_parquet.csv > middle10_CSVOut_mutualEntropyTweetToUser.csv
echo "middle10_MIP(Contain=T|ContainHashtag=T)"
sed -n '5591700, 5591710p' CSVOut_ProbTweetTrueContainHashtagTrue_1_parquet.csv > middle10_ProbTweetTrueContainHashtagTrue.csv
echo "middle10_MIP(Contain=T|FromUser=T)"
sed -n '17116236, 17116246p' CSVOut_ProbTweetTrueFromUserTrue_1_parquet.csv > middle10_ProbTweetTrueFromUserTrue.csv
echo "middle10_MIP(Contain=T|ToUser=T)"
sed -n '6717192, 6717202p' CSVOut_ProbTweetTrueToUserTrue_1_parquet.csv > middle10_ProbTweetTrueToUserTrue.csv

echo tail10_CEFromHashtag
sed -n '11183400,11183410p' CSVOut_CondEntropyTweetFromHashtag_parquet.csv > tail10_CSVOut_CondEntropyTweetContainHashtag.csv
echo tail10_CEToUser
sed -n '13434385,13434395p' CSVOut_CondEntropyTweetToUser_parquet.csv > tail10_CSVOut_CondEntropyTweetToUser.csv
echo tail10_CEFromUser
sed -n '34232472,34232482p' CSVOut_CondEntropyTweetFromUser_parquet.csv > tail10_CSVOut_CondEntropyTweetFromUser.csv
echo tail10_MIFromHashtag
sed -n '11183400,11183410p' CSVOut_mutualEntropyTweetFromHashtag_parquet.csv > tail10_CSVOut_mutualEntropyTweetContainHashtag.csv
echo tail10_MIFromUser
sed -n '34232472,34232482p' CSVOut_mutualEntropyTweetFromUser_parquet.csv > tail10_CSVOut_mutualEntropyTweetFromUser.csv
echo tail10_MIToUser
sed -n '13434385,13434395p' CSVOut_mutualEntropyTweetToUser_parquet.csv > tail10_CSVOut_mutualEntropyTweetToUser.csv
echo "tail10_MIP(Contain=T|ContainHashtag=T)"
sed -n '11183400,11183410p' CSVOut_ProbTweetTrueContainHashtagTrue_1_parquet.csv > tail10_ProbTweetTrueContainHashtagTrue.csv
echo "tail10_MIP(Contain=T|FromUser=T)"
sed -n '34232472,34232482p' CSVOut_ProbTweetTrueFromUserTrue_1_parquet.csv > tail10_ProbTweetTrueFromUserTrue.csv
echo "tail10_MIP(Contain=T|ToUser=T)"
sed -n '13434385,13434395p' CSVOut_ProbTweetTrueToUserTrue_1_parquet.csv > tail10_ProbTweetTrueToUserTrue.csv


echo middle10_CEFromHashtag
sed -n '3756, 3765p' NoZero_CSVOut_CondEntropyTweetFromHashtag_parquet.csv >  middle10_CSVOut_NoZero_CondEntropyTweetContainHashtag.csv
echo middle10_CEToUser
sed -n '5115547, 5115556p' NoZero_CSVOut_CondEntropyTweetToUser_parquet.csv > middle10_CSVOut_NoZero_CondEntropyTweetToUser.csv
echo middle10_CEFromUser
sed -n '13304605, 13304614p' NoZero_CSVOut_CondEntropyTweetFromUser_parquet.csv > middle10_NoZero_CSVOut_CondEntropyTweetFromUser.csv
echo middle10_MIFromHashtag
echo "middle10_MIP(Contain=T|ContainHashtag=T)"
sed -n '4351, 4360p' NoZero_CSVOut_ProbTweetTrueContainHashtagTrue_1_parquet.csv > middle10_NoZero_ProbTweetTrueContainHashtagTrue.csv
echo "middle10_MIP(Contain=T|FromUser=T)"
sed -n '25345, 25354p' NoZero_CSVOut_ProbTweetTrueFromUserTrue_1_parquet.csv > middle10_NoZero_ProbTweetTrueFromUserTrue.csv
echo "middle10_MIP(Contain=T|ToUser=T)"
sed -n '8915, 8924p' NoZero_CSVOut_ProbTweetTrueToUserTrue_1_parquet.csv > middle10_NoZero_ProbTweetTrueToUserTrue.csv

echo tail10_CEFromHashtag
sed -n '7514, 7523p' NoZero_CSVOut_CondEntropyTweetFromHashtag_parquet.csv > tail10_NoZero_CSVOut_CondEntropyTweetContainHashtag.csv
echo tail10_CEToUser
sed -n '10231096, 10231105p' NoZero_CSVOut_CondEntropyTweetToUser_parquet.csv > tail10_NoZero_CSVOut_CondEntropyTweetToUser.csv
echo tail10_CEFromUser
sed -n '26609211, 26609220p' NoZero_CSVOut_CondEntropyTweetFromUser_parquet.csv > tail10_NoZero_CSVOut_CondEntropyTweetFromUser.csv
echo "tail10_MIP(Contain=T|ContainHashtag=T)"
sed -n '8704, 8713p' NoZero_CSVOut_ProbTweetTrueContainHashtagTrue_1_parquet.csv > tail10_NoZero_ProbTweetTrueContainHashtagTrue.csv
echo "tail10_MIP(Contain=T|FromUser=T)"
sed -n '50692, 50701p' NoZero_CSVOut_ProbTweetTrueFromUserTrue_1_parquet.csv > tail10_NoZero_ProbTweetTrueFromUserTrue.csv
echo "tail10_MIP(Contain=T|ToUser=T)"
sed -n '17832, 17841p' NoZero_CSVOut_ProbTweetTrueToUserTrue_1_parquet.csv > tail10_NoZero_ProbTweetTrueToUserTrue.csv
