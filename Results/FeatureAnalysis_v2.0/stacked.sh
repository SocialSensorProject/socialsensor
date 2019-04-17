awk  '{print "H\t"$0}' data/Cele_death_Dataset_hashtag_features.txt > data/tmp.txt
awk  '{print "L\t"$0}'  data/Cele_death_Dataset_loc_features.txt >> data/tmp.txt
awk  '{print "M\t"$0}'  data/Cele_death_Dataset_mention_features.txt >> data/tmp.txt
awk  '{print "T\t"$0}'  data/Cele_death_Dataset_term_features.txt >> data/tmp.txt
awk  '{print "U\t"$0}'  data/Cele_death_Dataset_user_features.txt >> data/tmp.txt
sort -g -r -k3   data/tmp.txt > data/tmp2.txt

v1=$(head -n 10 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10" | bc)
v2=$(head -n 10 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10" | bc)
v3=$(head -n 10 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10" | bc)
v4=$(head -n 10 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10" | bc)
v5=$(head -n 10 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10" | bc)

echo -e '0.001%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 > data/Cele_death_stacked_histo.txt

v1=$(head -n 100 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100" | bc)
v2=$(head -n 100 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100" | bc)
v3=$(head -n 100 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100" | bc)
v4=$(head -n 100 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100" | bc)
v5=$(head -n 100 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100" | bc)

echo -e '0.01%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Cele_death_stacked_histo.txt

v1=$(head -n 1000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 1000" | bc)
v2=$(head -n 1000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 1000" | bc)
v3=$(head -n 1000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 1000" | bc)
v4=$(head -n 1000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 1000" | bc)
v5=$(head -n 1000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 1000" | bc)

echo -e '0.1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Cele_death_stacked_histo.txt


v1=$(head -n 10000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10000" | bc)
v2=$(head -n 10000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10000" | bc)
v3=$(head -n 10000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10000" | bc)
v4=$(head -n 10000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10000" | bc)
v5=$(head -n 10000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10000" | bc)

echo -e '1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Cele_death_stacked_histo.txt

v1=$(head -n 100000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100000" | bc)
v2=$(head -n 100000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100000" | bc)
v3=$(head -n 100000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100000" | bc)
v4=$(head -n 100000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100000" | bc)
v5=$(head -n 100000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100000" | bc)

echo -e '10%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Cele_death_stacked_histo.txt

rm data/tmp.txt
rm data/tmp2.txt

awk  '{print "H\t"$0}' data/Health_Dataset_hashtag_features.txt > data/tmp.txt
awk  '{print "L\t"$0}'  data/Health_Dataset_loc_features.txt >> data/tmp.txt
awk  '{print "M\t"$0}'  data/Health_Dataset_mention_features.txt >> data/tmp.txt
awk  '{print "T\t"$0}'  data/Health_Dataset_term_features.txt >> data/tmp.txt
awk  '{print "U\t"$0}'  data/Health_Dataset_user_features.txt >> data/tmp.txt
sort -g -r -k3   data/tmp.txt > data/tmp2.txt

v1=$(head -n 10 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10" | bc)
v2=$(head -n 10 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10" | bc)
v3=$(head -n 10 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10" | bc)
v4=$(head -n 10 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10" | bc)
v5=$(head -n 10 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10" | bc)

echo -e '0.001%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 > data/Health_stacked_histo.txt

v1=$(head -n 100 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100" | bc)
v2=$(head -n 100 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100" | bc)
v3=$(head -n 100 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100" | bc)
v4=$(head -n 100 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100" | bc)
v5=$(head -n 100 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100" | bc)

echo -e '0.01%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Health_stacked_histo.txt

v1=$(head -n 1000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 1000" | bc)
v2=$(head -n 1000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 1000" | bc)
v3=$(head -n 1000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 1000" | bc)
v4=$(head -n 1000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 1000" | bc)
v5=$(head -n 1000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 1000" | bc)

echo -e '0.1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Health_stacked_histo.txt


v1=$(head -n 10000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10000" | bc)
v2=$(head -n 10000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10000" | bc)
v3=$(head -n 10000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10000" | bc)
v4=$(head -n 10000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10000" | bc)
v5=$(head -n 10000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10000" | bc)

echo -e '1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Health_stacked_histo.txt

v1=$(head -n 100000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100000" | bc)
v2=$(head -n 100000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100000" | bc)
v3=$(head -n 100000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100000" | bc)
v4=$(head -n 100000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100000" | bc)
v5=$(head -n 100000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100000" | bc)

echo -e '10%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Health_stacked_histo.txt

rm data/tmp.txt
rm data/tmp2.txt

awk  '{print "H\t"$0}' data/Human_Disaster_Dataset_hashtag_features.txt > data/tmp.txt
awk  '{print "L\t"$0}'  data/Human_Disaster_Dataset_loc_features.txt >> data/tmp.txt
awk  '{print "M\t"$0}'  data/Human_Disaster_Dataset_mention_features.txt >> data/tmp.txt
awk  '{print "T\t"$0}'  data/Human_Disaster_Dataset_term_features.txt >> data/tmp.txt
awk  '{print "U\t"$0}'  data/Human_Disaster_Dataset_user_features.txt >> data/tmp.txt
sort -g -r -k3   data/tmp.txt > data/tmp2.txt

v1=$(head -n 10 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10" | bc)
v2=$(head -n 10 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10" | bc)
v3=$(head -n 10 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10" | bc)
v4=$(head -n 10 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10" | bc)
v5=$(head -n 10 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10" | bc)

echo -e '0.001%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 > data/Human_Disaster_stacked_histo.txt

v1=$(head -n 100 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100" | bc)
v2=$(head -n 100 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100" | bc)
v3=$(head -n 100 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100" | bc)
v4=$(head -n 100 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100" | bc)
v5=$(head -n 100 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100" | bc)

echo -e '0.01%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Human_Disaster_stacked_histo.txt

v1=$(head -n 1000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 1000" | bc)
v2=$(head -n 1000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 1000" | bc)
v3=$(head -n 1000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 1000" | bc)
v4=$(head -n 1000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 1000" | bc)
v5=$(head -n 1000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 1000" | bc)

echo -e '0.1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Human_Disaster_stacked_histo.txt


v1=$(head -n 10000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10000" | bc)
v2=$(head -n 10000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10000" | bc)
v3=$(head -n 10000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10000" | bc)
v4=$(head -n 10000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10000" | bc)
v5=$(head -n 10000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10000" | bc)

echo -e '1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Human_Disaster_stacked_histo.txt

v1=$(head -n 100000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100000" | bc)
v2=$(head -n 100000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100000" | bc)
v3=$(head -n 100000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100000" | bc)
v4=$(head -n 100000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100000" | bc)
v5=$(head -n 100000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100000" | bc)

echo -e '10%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Human_Disaster_stacked_histo.txt

rm data/tmp.txt
rm data/tmp2.txt

awk  '{print "H\t"$0}' data/Iran_Dataset_hashtag_features.txt > data/tmp.txt
awk  '{print "L\t"$0}'  data/Iran_Dataset_loc_features.txt >> data/tmp.txt
awk  '{print "M\t"$0}'  data/Iran_Dataset_mention_features.txt >> data/tmp.txt
awk  '{print "T\t"$0}'  data/Iran_Dataset_term_features.txt >> data/tmp.txt
awk  '{print "U\t"$0}'  data/Iran_Dataset_user_features.txt >> data/tmp.txt
sort -g -r -k3   data/tmp.txt > data/tmp2.txt

v1=$(head -n 10 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10" | bc)
v2=$(head -n 10 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10" | bc)
v3=$(head -n 10 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10" | bc)
v4=$(head -n 10 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10" | bc)
v5=$(head -n 10 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10" | bc)

echo -e '0.001%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 > data/Iran_stacked_histo.txt

v1=$(head -n 100 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100" | bc)
v2=$(head -n 100 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100" | bc)
v3=$(head -n 100 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100" | bc)
v4=$(head -n 100 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100" | bc)
v5=$(head -n 100 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100" | bc)

echo -e '0.01%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Iran_stacked_histo.txt

v1=$(head -n 1000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 1000" | bc)
v2=$(head -n 1000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 1000" | bc)
v3=$(head -n 1000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 1000" | bc)
v4=$(head -n 1000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 1000" | bc)
v5=$(head -n 1000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 1000" | bc)

echo -e '0.1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Iran_stacked_histo.txt


v1=$(head -n 10000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10000" | bc)
v2=$(head -n 10000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10000" | bc)
v3=$(head -n 10000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10000" | bc)
v4=$(head -n 10000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10000" | bc)
v5=$(head -n 10000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10000" | bc)

echo -e '1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Iran_stacked_histo.txt

v1=$(head -n 100000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100000" | bc)
v2=$(head -n 100000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100000" | bc)
v3=$(head -n 100000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100000" | bc)
v4=$(head -n 100000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100000" | bc)
v5=$(head -n 100000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100000" | bc)

echo -e '10%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Iran_stacked_histo.txt

rm data/tmp.txt
rm data/tmp2.txt

awk  '{print "H\t"$0}' data/LGBT_Dataset_hashtag_features.txt > data/tmp.txt
awk  '{print "L\t"$0}'  data/LGBT_Dataset_loc_features.txt >> data/tmp.txt
awk  '{print "M\t"$0}'  data/LGBT_Dataset_mention_features.txt >> data/tmp.txt
awk  '{print "T\t"$0}'  data/LGBT_Dataset_term_features.txt >> data/tmp.txt
awk  '{print "U\t"$0}'  data/LGBT_Dataset_user_features.txt >> data/tmp.txt
sort -g -r -k3   data/tmp.txt > data/tmp2.txt

v1=$(head -n 10 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10" | bc)
v2=$(head -n 10 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10" | bc)
v3=$(head -n 10 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10" | bc)
v4=$(head -n 10 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10" | bc)
v5=$(head -n 10 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10" | bc)

echo -e '0.001%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 > data/LGBT_stacked_histo.txt

v1=$(head -n 100 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100" | bc)
v2=$(head -n 100 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100" | bc)
v3=$(head -n 100 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100" | bc)
v4=$(head -n 100 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100" | bc)
v5=$(head -n 100 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100" | bc)

echo -e '0.01%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/LGBT_stacked_histo.txt

v1=$(head -n 1000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 1000" | bc)
v2=$(head -n 1000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 1000" | bc)
v3=$(head -n 1000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 1000" | bc)
v4=$(head -n 1000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 1000" | bc)
v5=$(head -n 1000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 1000" | bc)

echo -e '0.1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/LGBT_stacked_histo.txt


v1=$(head -n 10000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10000" | bc)
v2=$(head -n 10000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10000" | bc)
v3=$(head -n 10000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10000" | bc)
v4=$(head -n 10000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10000" | bc)
v5=$(head -n 10000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10000" | bc)

echo -e '1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/LGBT_stacked_histo.txt

v1=$(head -n 100000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100000" | bc)
v2=$(head -n 100000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100000" | bc)
v3=$(head -n 100000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100000" | bc)
v4=$(head -n 100000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100000" | bc)
v5=$(head -n 100000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100000" | bc)

echo -e '10%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/LGBT_stacked_histo.txt

rm data/tmp.txt
rm data/tmp2.txt

awk  '{print "H\t"$0}' data/Natr_Disaster_Dataset_hashtag_features.txt > data/tmp.txt
awk  '{print "L\t"$0}'  data/Natr_Disaster_Dataset_loc_features.txt >> data/tmp.txt
awk  '{print "M\t"$0}'  data/Natr_Disaster_Dataset_mention_features.txt >> data/tmp.txt
awk  '{print "T\t"$0}'  data/Natr_Disaster_Dataset_term_features.txt >> data/tmp.txt
awk  '{print "U\t"$0}'  data/Natr_Disaster_Dataset_user_features.txt >> data/tmp.txt
sort -g -r -k3   data/tmp.txt > data/tmp2.txt

v1=$(head -n 10 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10" | bc)
v2=$(head -n 10 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10" | bc)
v3=$(head -n 10 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10" | bc)
v4=$(head -n 10 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10" | bc)
v5=$(head -n 10 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10" | bc)

echo -e '0.001%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 > data/Natr_Disaster_stacked_histo.txt

v1=$(head -n 100 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100" | bc)
v2=$(head -n 100 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100" | bc)
v3=$(head -n 100 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100" | bc)
v4=$(head -n 100 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100" | bc)
v5=$(head -n 100 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100" | bc)

echo -e '0.01%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Natr_Disaster_stacked_histo.txt

v1=$(head -n 1000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 1000" | bc)
v2=$(head -n 1000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 1000" | bc)
v3=$(head -n 1000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 1000" | bc)
v4=$(head -n 1000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 1000" | bc)
v5=$(head -n 1000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 1000" | bc)

echo -e '0.1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Natr_Disaster_stacked_histo.txt


v1=$(head -n 10000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10000" | bc)
v2=$(head -n 10000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10000" | bc)
v3=$(head -n 10000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10000" | bc)
v4=$(head -n 10000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10000" | bc)
v5=$(head -n 10000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10000" | bc)

echo -e '1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Natr_Disaster_stacked_histo.txt

v1=$(head -n 100000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100000" | bc)
v2=$(head -n 100000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100000" | bc)
v3=$(head -n 100000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100000" | bc)
v4=$(head -n 100000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100000" | bc)
v5=$(head -n 100000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100000" | bc)

echo -e '10%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Natr_Disaster_stacked_histo.txt

rm data/tmp.txt
rm data/tmp2.txt

awk  '{print "H\t"$0}' data/Soccer_Dataset_hashtag_features.txt > data/tmp.txt
awk  '{print "L\t"$0}'  data/Soccer_Dataset_loc_features.txt >> data/tmp.txt
awk  '{print "M\t"$0}'  data/Soccer_Dataset_mention_features.txt >> data/tmp.txt
awk  '{print "T\t"$0}'  data/Soccer_Dataset_term_features.txt >> data/tmp.txt
awk  '{print "U\t"$0}'  data/Soccer_Dataset_user_features.txt >> data/tmp.txt
sort -g -r -k3   data/tmp.txt > data/tmp2.txt

v1=$(head -n 10 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10" | bc)
v2=$(head -n 10 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10" | bc)
v3=$(head -n 10 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10" | bc)
v4=$(head -n 10 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10" | bc)
v5=$(head -n 10 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10" | bc)

echo -e '0.001%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 > data/Soccer_stacked_histo.txt

v1=$(head -n 100 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100" | bc)
v2=$(head -n 100 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100" | bc)
v3=$(head -n 100 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100" | bc)
v4=$(head -n 100 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100" | bc)
v5=$(head -n 100 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100" | bc)

echo -e '0.01%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Soccer_stacked_histo.txt

v1=$(head -n 1000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 1000" | bc)
v2=$(head -n 1000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 1000" | bc)
v3=$(head -n 1000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 1000" | bc)
v4=$(head -n 1000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 1000" | bc)
v5=$(head -n 1000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 1000" | bc)

echo -e '0.1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Soccer_stacked_histo.txt


v1=$(head -n 10000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10000" | bc)
v2=$(head -n 10000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10000" | bc)
v3=$(head -n 10000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10000" | bc)
v4=$(head -n 10000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10000" | bc)
v5=$(head -n 10000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10000" | bc)

echo -e '1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Soccer_stacked_histo.txt

v1=$(head -n 100000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100000" | bc)
v2=$(head -n 100000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100000" | bc)
v3=$(head -n 100000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100000" | bc)
v4=$(head -n 100000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100000" | bc)
v5=$(head -n 100000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100000" | bc)

echo -e '10%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Soccer_stacked_histo.txt

rm data/tmp.txt
rm data/tmp2.txt

awk  '{print "H\t"$0}' data/Social_issue_Dataset_hashtag_features.txt > data/tmp.txt
awk  '{print "L\t"$0}'  data/Social_issue_Dataset_loc_features.txt >> data/tmp.txt
awk  '{print "M\t"$0}'  data/Social_issue_Dataset_mention_features.txt >> data/tmp.txt
awk  '{print "T\t"$0}'  data/Social_issue_Dataset_term_features.txt >> data/tmp.txt
awk  '{print "U\t"$0}'  data/Social_issue_Dataset_user_features.txt >> data/tmp.txt
sort -g -r -k3   data/tmp.txt > data/tmp2.txt

v1=$(head -n 10 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10" | bc)
v2=$(head -n 10 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10" | bc)
v3=$(head -n 10 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10" | bc)
v4=$(head -n 10 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10" | bc)
v5=$(head -n 10 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10" | bc)

echo -e '0.001%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 > data/Social_issue_stacked_histo.txt

v1=$(head -n 100 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100" | bc)
v2=$(head -n 100 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100" | bc)
v3=$(head -n 100 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100" | bc)
v4=$(head -n 100 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100" | bc)
v5=$(head -n 100 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100" | bc)

echo -e '0.01%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Social_issue_stacked_histo.txt

v1=$(head -n 1000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 1000" | bc)
v2=$(head -n 1000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 1000" | bc)
v3=$(head -n 1000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 1000" | bc)
v4=$(head -n 1000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 1000" | bc)
v5=$(head -n 1000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 1000" | bc)

echo -e '0.1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Social_issue_stacked_histo.txt


v1=$(head -n 10000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10000" | bc)
v2=$(head -n 10000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10000" | bc)
v3=$(head -n 10000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10000" | bc)
v4=$(head -n 10000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10000" | bc)
v5=$(head -n 10000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10000" | bc)

echo -e '1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Social_issue_stacked_histo.txt

v1=$(head -n 100000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100000" | bc)
v2=$(head -n 100000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100000" | bc)
v3=$(head -n 100000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100000" | bc)
v4=$(head -n 100000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100000" | bc)
v5=$(head -n 100000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100000" | bc)

echo -e '10%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Social_issue_stacked_histo.txt

rm data/tmp.txt
rm data/tmp2.txt

awk  '{print "H\t"$0}' data/Space_Dataset_hashtag_features.txt > data/tmp.txt
awk  '{print "L\t"$0}'  data/Space_Dataset_loc_features.txt >> data/tmp.txt
awk  '{print "M\t"$0}'  data/Space_Dataset_mention_features.txt >> data/tmp.txt
awk  '{print "T\t"$0}'  data/Space_Dataset_term_features.txt >> data/tmp.txt
awk  '{print "U\t"$0}'  data/Space_Dataset_user_features.txt >> data/tmp.txt
sort -g -r -k3   data/tmp.txt > data/tmp2.txt

v1=$(head -n 10 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10" | bc)
v2=$(head -n 10 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10" | bc)
v3=$(head -n 10 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10" | bc)
v4=$(head -n 10 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10" | bc)
v5=$(head -n 10 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10" | bc)

echo -e '0.001%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 > data/Space_stacked_histo.txt

v1=$(head -n 100 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100" | bc)
v2=$(head -n 100 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100" | bc)
v3=$(head -n 100 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100" | bc)
v4=$(head -n 100 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100" | bc)
v5=$(head -n 100 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100" | bc)

echo -e '0.01%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Space_stacked_histo.txt

v1=$(head -n 1000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 1000" | bc)
v2=$(head -n 1000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 1000" | bc)
v3=$(head -n 1000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 1000" | bc)
v4=$(head -n 1000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 1000" | bc)
v5=$(head -n 1000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 1000" | bc)

echo -e '0.1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Space_stacked_histo.txt


v1=$(head -n 10000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10000" | bc)
v2=$(head -n 10000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10000" | bc)
v3=$(head -n 10000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10000" | bc)
v4=$(head -n 10000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10000" | bc)
v5=$(head -n 10000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10000" | bc)

echo -e '1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Space_stacked_histo.txt

v1=$(head -n 100000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100000" | bc)
v2=$(head -n 100000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100000" | bc)
v3=$(head -n 100000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100000" | bc)
v4=$(head -n 100000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100000" | bc)
v5=$(head -n 100000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100000" | bc)

echo -e '10%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Space_stacked_histo.txt

rm data/tmp.txt
rm data/tmp2.txt

awk  '{print "H\t"$0}' data/Tennis_Dataset_hashtag_features.txt > data/tmp.txt
awk  '{print "L\t"$0}'  data/Tennis_Dataset_loc_features.txt >> data/tmp.txt
awk  '{print "M\t"$0}'  data/Tennis_Dataset_mention_features.txt >> data/tmp.txt
awk  '{print "T\t"$0}'  data/Tennis_Dataset_term_features.txt >> data/tmp.txt
awk  '{print "U\t"$0}'  data/Tennis_Dataset_user_features.txt >> data/tmp.txt
sort -g -r -k3   data/tmp.txt > data/tmp2.txt

v1=$(head -n 10 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10" | bc)
v2=$(head -n 10 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10" | bc)
v3=$(head -n 10 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10" | bc)
v4=$(head -n 10 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10" | bc)
v5=$(head -n 10 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10" | bc)

echo -e '0.001%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 > data/Tennis_stacked_histo.txt

v1=$(head -n 100 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100" | bc)
v2=$(head -n 100 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100" | bc)
v3=$(head -n 100 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100" | bc)
v4=$(head -n 100 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100" | bc)
v5=$(head -n 100 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100" | bc)

echo -e '0.01%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Tennis_stacked_histo.txt

v1=$(head -n 1000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 1000" | bc)
v2=$(head -n 1000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 1000" | bc)
v3=$(head -n 1000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 1000" | bc)
v4=$(head -n 1000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 1000" | bc)
v5=$(head -n 1000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 1000" | bc)

echo -e '0.1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Tennis_stacked_histo.txt


v1=$(head -n 10000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 10000" | bc)
v2=$(head -n 10000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 10000" | bc)
v3=$(head -n 10000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 10000" | bc)
v4=$(head -n 10000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 10000" | bc)
v5=$(head -n 10000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 10000" | bc)

echo -e '1%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Tennis_stacked_histo.txt

v1=$(head -n 100000 data/tmp2.txt | grep 'H' | wc -l )
v1=$(echo "scale = 2 ; $v1 * 100 / 100000" | bc)
v2=$(head -n 100000 data/tmp2.txt | grep 'L' | wc -l )
v2=$(echo "scale = 2 ; $v2 * 100 / 100000" | bc)
v3=$(head -n 100000 data/tmp2.txt | grep 'M' | wc -l )
v3=$(echo "scale = 2 ; $v3 * 100 / 100000" | bc)
v4=$(head -n 100000 data/tmp2.txt | grep 'T' | wc -l )
v4=$(echo "scale = 2 ; $v4 * 100 / 100000" | bc)
v5=$(head -n 100000 data/tmp2.txt | grep 'U' | wc -l )
v5=$(echo "scale = 2 ; $v5 * 100 / 100000" | bc)

echo -e '10%\t'$v1'\t'$v2'\t'$v3'\t'$v4'\t'$v5 >> data/Tennis_stacked_histo.txt

rm data/tmp.txt
rm data/tmp2.txt

