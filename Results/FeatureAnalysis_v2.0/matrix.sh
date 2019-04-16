v51=$(head -n 100000 data/Tennis_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }' | awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v51"\c" > data/matrix.txt
v52=$(head -n 100000 data/Space_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}')
echo -e $v52"\c" >> data/matrix.txt
v53=$(head -n 100000 data/Soccer_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v53"\c" >> data/matrix.txt
v54=$(head -n 100000 data/Iran_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v54"\c" >> data/matrix.txt
v55=$(head -n 100000 data/Human_Disaster_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v55"\c" >> data/matrix.txt
v56=$(head -n 100000 data/Cele_death_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v56"\c" >> data/matrix.txt
v57=$(head -n 100000 data/Social_issue_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v57"\c" >> data/matrix.txt
v58=$(head -n 100000 data/Natr_Disaster_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v58"\c" >> data/matrix.txt
v59=$(head -n 100000 data/Health_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v59"\c" >> data/matrix.txt
v510=$(head -n 100000 data/LGBT_Dataset_term_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v510"\c" >> data/matrix.txt
echo $v51 $v52 $v53 $v54 $v55 $v56 $v57 $v58 $v59 $v510 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}' | awk  '{printf "%0.2f\n",$1"."substr($2,1,2)}'  >> data/matrix.txt

v41=$(head -n 100000 data/Tennis_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v41"\c" >> data/matrix.txt
v42=$(head -n 100000 data/Space_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}')
echo -e $v42"\c" >> data/matrix.txt
v43=$(head -n 100000 data/Soccer_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v43"\c" >> data/matrix.txt
v44=$(head -n 100000 data/Iran_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v44"\c" >> data/matrix.txt
v45=$(head -n 100000 data/Human_Disaster_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v45"\c" >> data/matrix.txt
v46=$(head -n 100000 data/Cele_death_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v46"\c" >> data/matrix.txt
v47=$(head -n 100000 data/Social_issue_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v47"\c" >> data/matrix.txt
v48=$(head -n 100000 data/Natr_Disaster_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v48"\c" >> data/matrix.txt
v49=$(head -n 100000 data/Health_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v49"\c" >> data/matrix.txt
v410=$(head -n 100000 data/LGBT_Dataset_loc_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v410"\c" >> data/matrix.txt
echo $v41 $v42 $v43 $v44 $v45 $v46 $v47 $v48 $v49 $v410 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}'  | awk  '{printf "%0.2f\n",$1"."substr($2,1,2)}' >> data/matrix.txt

v31=$(head -n 100000 data/Tennis_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v31"\c" >> data/matrix.txt
v32=$(head -n 100000 data/Space_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}')
echo -e $v32"\c" >> data/matrix.txt
v33=$(head -n 100000 data/Soccer_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v33"\c" >> data/matrix.txt
v34=$(head -n 100000 data/Iran_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v34"\c" >> data/matrix.txt
v35=$(head -n 100000 data/Human_Disaster_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v35"\c" >> data/matrix.txt
v36=$(head -n 100000 data/Cele_death_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v36"\c" >> data/matrix.txt
v37=$(head -n 100000 data/Social_issue_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v37"\c" >> data/matrix.txt
v38=$(head -n 100000 data/Natr_Disaster_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v38"\c" >> data/matrix.txt
v39=$(head -n 100000 data/Health_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v39"\c" >> data/matrix.txt
v310=$(head -n 100000 data/LGBT_Dataset_user_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v310"\c" >> data/matrix.txt
echo $v31 $v32 $v33 $v34 $v35 $v36 $v37 $v38 $v39 $v310 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}'  | awk  '{printf "%0.2f\n",$1"."substr($2,1,2)}' >> data/matrix.txt

v21=$(head -n 100000 data/Tennis_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v21"\c" >> data/matrix.txt
v22=$(head -n 100000 data/Space_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}')
echo -e $v22"\c" >> data/matrix.txt
v23=$(head -n 100000 data/Soccer_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v23"\c" >> data/matrix.txt
v24=$(head -n 100000 data/Iran_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v24"\c" >> data/matrix.txt
v25=$(head -n 100000 data/Human_Disaster_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v25"\c" >> data/matrix.txt
v26=$(head -n 100000 data/Cele_death_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v26"\c" >> data/matrix.txt
v27=$(head -n 100000 data/Social_issue_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v27"\c" >> data/matrix.txt
v28=$(head -n 100000 data/Natr_Disaster_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v28"\c" >> data/matrix.txt
v29=$(head -n 100000 data/Health_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v29"\c" >> data/matrix.txt
v210=$(head -n 100000 data/LGBT_Dataset_hashtag_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v210"\c" >> data/matrix.txt
echo $v21 $v22 $v23 $v24 $v25 $v26 $v27 $v28 $v29 $v210 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}'  | awk  '{printf "%0.2f\n",$1"."substr($2,1,2)}' >> data/matrix.txt

v11=$(head -n 100000 data/Tennis_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v11"\c" >> data/matrix.txt
v12=$(head -n 100000 data/Space_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}')
echo -e $v12"\c" >> data/matrix.txt
v13=$(head -n 100000 data/Soccer_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v13"\c" >> data/matrix.txt
v14=$(head -n 100000 data/Iran_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v14"\c" >> data/matrix.txt
v15=$(head -n 100000 data/Human_Disaster_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v15"\c" >> data/matrix.txt
v16=$(head -n 100000 data/Cele_death_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v16"\c" >> data/matrix.txt
v17=$(head -n 100000 data/Social_issue_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v17"\c" >> data/matrix.txt
v18=$(head -n 100000 data/Natr_Disaster_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v18"\c" >> data/matrix.txt
v19=$(head -n 100000 data/Health_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v19"\c" >> data/matrix.txt
v110=$(head -n 100000 data/LGBT_Dataset_mention_features.txt | awk '{ sum += $2 } END { printf sum*100000000 / NR"\t" }'| awk  '{printf "%0.2f\t",$1"."substr($2,1,2)}') 
echo -e $v110"\c" >> data/matrix.txt
echo $v11 $v12 $v13 $v14 $v15 $v16 $v17 $v18 $v19 $v110 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}'  | awk  '{printf "%0.2f\n",$1"."substr($2,1,2)}' >> data/matrix.txt

v61=$(echo $v11 $v21 $v31 $v41 $v51 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}')
v62=$(echo $v12 $v22 $v32 $v42 $v52 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}')
v63=$(echo $v13 $v23 $v33 $v43 $v53 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}')
v64=$(echo $v14 $v24 $v34 $v44 $v54 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}')
v65=$(echo $v15 $v25 $v35 $v45 $v55 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}')
v66=$(echo $v16 $v26 $v36 $v46 $v56 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}')
v67=$(echo $v17 $v27 $v37 $v47 $v57 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}')
v68=$(echo $v18 $v28 $v38 $v48 $v58 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}')
v69=$(echo $v19 $v29 $v39 $v49 $v59 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}')
v610=$(echo $v110 $v210 $v310 $v410 $v510 | awk -v RS=' ' '{sum+=$1; count++} END{print  sum/count}')

echo $v61 $v62 $v63 $v64 $v65 $v66 $v67 $v68 $v69 $v610 - | cat - data/matrix.txt > data/temp && mv data/temp data/matrix.txt













