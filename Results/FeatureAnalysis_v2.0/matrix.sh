v1=$(head -n 100000 data/Tennis_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }' | awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v1"\c" > data/matrix.txt
v2=$(head -n 100000 data/Space_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}')
echo -e $v2"\c" >> data/matrix.txt
v3=$(head -n 100000 data/Soccer_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v3"\c" >> data/matrix.txt
v4=$(head -n 100000 data/Iran_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v4"\c" >> data/matrix.txt
v5=$(head -n 100000 data/Human_Disaster_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v5"\c" >> data/matrix.txt
v6=$(head -n 100000 data/Cele_death_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v6"\c" >> data/matrix.txt
v7=$(head -n 100000 data/Social_issue_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v7"\c" >> data/matrix.txt
v8=$(head -n 100000 data/Natr_Disaster_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v8"\c" >> data/matrix.txt
v9=$(head -n 100000 data/Health_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v9"\c" >> data/matrix.txt
v10=$(head -n 100000 data/LGBT_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v10"\c" >> data/matrix.txt
echo $v1 $v2 $v3 $v4 $v5 $v6 $v7 $v8 $v9 $v10 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}' | awk  '{printf "%0.2f\n",$1"."substr($2,1,2)}'  >> data/matrix.txt



v1=$(head -n 100000 data/Tennis_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v1"\c" >> data/matrix.txt
v2=$(head -n 100000 data/Space_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}')
echo -e $v2"\c" >> data/matrix.txt
v3=$(head -n 100000 data/Soccer_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v3"\c" >> data/matrix.txt
v4=$(head -n 100000 data/Iran_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v4"\c" >> data/matrix.txt
v5=$(head -n 100000 data/Human_Disaster_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v5"\c" >> data/matrix.txt
v6=$(head -n 100000 data/Cele_death_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v6"\c" >> data/matrix.txt
v7=$(head -n 100000 data/Social_issue_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v7"\c" >> data/matrix.txt
v8=$(head -n 100000 data/Natr_Disaster_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v8"\c" >> data/matrix.txt
v9=$(head -n 100000 data/Health_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v9"\c" >> data/matrix.txt
v10=$(head -n 100000 data/LGBT_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v10"\c" >> data/matrix.txt
echo $v1 $v2 $v3 $v4 $v5 $v6 $v7 $v8 $v9 $v10 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}'  | awk  '{printf "%0.2f\n",$1"."substr($2,1,2)}' >> data/matrix.txt

v1=$(head -n 100000 data/Tennis_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v1"\c" >> data/matrix.txt
v2=$(head -n 100000 data/Space_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}')
echo -e $v2"\c" >> data/matrix.txt
v3=$(head -n 100000 data/Soccer_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v3"\c" >> data/matrix.txt
v4=$(head -n 100000 data/Iran_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v4"\c" >> data/matrix.txt
v5=$(head -n 100000 data/Human_Disaster_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v5"\c" >> data/matrix.txt
v6=$(head -n 100000 data/Cele_death_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v6"\c" >> data/matrix.txt
v7=$(head -n 100000 data/Social_issue_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v7"\c" >> data/matrix.txt
v8=$(head -n 100000 data/Natr_Disaster_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v8"\c" >> data/matrix.txt
v9=$(head -n 100000 data/Health_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v9"\c" >> data/matrix.txt
v10=$(head -n 100000 data/LGBT_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v10"\c" >> data/matrix.txt
echo $v1 $v2 $v3 $v4 $v5 $v6 $v7 $v8 $v9 $v10 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}'  | awk  '{printf "%0.2f\n",$1"."substr($2,1,2)}' >> data/matrix.txt

v1=$(head -n 100000 data/Tennis_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v1"\c" >> data/matrix.txt
v2=$(head -n 100000 data/Space_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}')
echo -e $v2"\c" >> data/matrix.txt
v3=$(head -n 100000 data/Soccer_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v3"\c" >> data/matrix.txt
v4=$(head -n 100000 data/Iran_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v4"\c" >> data/matrix.txt
v5=$(head -n 100000 data/Human_Disaster_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v5"\c" >> data/matrix.txt
v6=$(head -n 100000 data/Cele_death_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v6"\c" >> data/matrix.txt
v7=$(head -n 100000 data/Social_issue_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v7"\c" >> data/matrix.txt
v8=$(head -n 100000 data/Natr_Disaster_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v8"\c" >> data/matrix.txt
v9=$(head -n 100000 data/Health_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v9"\c" >> data/matrix.txt
v10=$(head -n 100000 data/LGBT_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v10"\c" >> data/matrix.txt
echo $v1 $v2 $v3 $v4 $v5 $v6 $v7 $v8 $v9 $v10 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}'  | awk  '{printf "%0.2f\n",$1"."substr($2,1,2)}' >> data/matrix.txt

v1=$(head -n 100000 data/Tennis_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v1"\c" >> data/matrix.txt
v2=$(head -n 100000 data/Space_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}')
echo -e $v2"\c" >> data/matrix.txt
v3=$(head -n 100000 data/Soccer_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v3"\c" >> data/matrix.txt
v4=$(head -n 100000 data/Iran_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v4"\c" >> data/matrix.txt
v5=$(head -n 100000 data/Human_Disaster_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v5"\c" >> data/matrix.txt
v6=$(head -n 100000 data/Cele_death_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v6"\c" >> data/matrix.txt
v7=$(head -n 100000 data/Social_issue_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v7"\c" >> data/matrix.txt
v8=$(head -n 100000 data/Natr_Disaster_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v8"\c" >> data/matrix.txt
v9=$(head -n 100000 data/Health_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v9"\c" >> data/matrix.txt
v10=$(head -n 100000 data/LGBT_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v10"\c" >> data/matrix.txt
echo $v1 $v2 $v3 $v4 $v5 $v6 $v7 $v8 $v9 $v10 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}'  | awk  '{printf "%0.2f\n",$1"."substr($2,1,2)}' >> data/matrix.txt

















