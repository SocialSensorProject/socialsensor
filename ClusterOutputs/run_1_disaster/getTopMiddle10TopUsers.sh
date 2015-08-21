#totalCEFromUser: 34232482
#totalCEToUser: 20125022
#totalCEFromHashtag: 11183410

#CE_FromUser: 17116236, 17116246
#CE_ToUser: 10062506, 10062516
#CE_FromHashtag: 5591700, 5591710

echo top10_CEFromHashtag
sed -n '1,10p' CSVOut_CondEntropyTweetFromHashtag_parquet.csv > top10_CSVOut_CondEntropyTweetFromHashtag.csv
echo top10_CEToUser
sed -n '1,10p' CSVOut_CondEntropyTweetToUser_parquet.csv > top10_CSVOut_CondEntropyTweetToUser.csv
echo top10_CEFromUser
sed -n '1,10p' CSVOut_CondEntropyTweetFromUser_parquet.csv > top10_CSVOut_CondEntropyTweetFromUser.csv
echo top10_MIFromHashtag
sed -n '1,10p' CSVOut_mutualEntropyTweetFromHashtag_parquet.csv > top10_CSVOut_mutualEntropyTweetFromHashtag.csv
echo top10_MIFromUser
sed -n '1,10p' CSVOut_mutualEntropyTweetFromUser_parquet.csv > top10_CSVOut_mutualEntropyTweetFromUser.csv
echo top10_MIToUser
sed -n '1,10p' CSVOut_mutualEntropyTweetToUser_parquet.csv > top10_CSVOut_mutualEntropyTweetToUser.csv

echo middle10_CEFromHashtag
sed -n '5591700, 5591710p' CSVOut_CondEntropyTweetFromHashtag_parquet.csv >  middle10_CSVOut_CondEntropyTweetFromHashtag.csv
echo middle10_CEToUser
sed -n '10062506, 10062516p' CSVOut_CondEntropyTweetToUser_parquet.csv > middle10_CSVOut_CondEntropyTweetToUser.csv
echo middle10_CEFromUser
sed -n '17116236, 17116246p' CSVOut_CondEntropyTweetFromUser_parquet.csv > middle10_CSVOut_CondEntropyTweetFromUser.csv
echo middle10_MIFromHashtag
sed -n '5591700, 5591710p' CSVOut_mutualEntropyTweetFromHashtag_parquet.csv > middle10_CSVOut_mutualEntropyTweetFromHashtag.csv
echo middle10_MIFromUser
sed -n '17116236, 17116246p' CSVOut_mutualEntropyTweetFromUser_parquet.csv > middle10_CSVOut_mutualEntropyTweetFromUser.csv
echo middle10_MIToUser
sed -n '10062506, 10062516p' CSVOut_mutualEntropyTweetToUser_parquet.csv > middle10_CSVOut_mutualEntropyTweetToUser.csv
