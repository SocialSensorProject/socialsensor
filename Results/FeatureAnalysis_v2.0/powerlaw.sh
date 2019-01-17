clear
reset
set ylabel "Mutual Information" font "Courier,22"
set grid
set key top
#set size 0.8,0.8
set logscale y
set logscale x
set format x "10^{%L}"
set format y "10^{%L}"
set term  png 

set output "term_features.png"
plot "< awk '{print FNR,$0}' data/Tennis_Train_term_features.txt" using 1:($2+(1.0/100000)) title 'Tenis'  with points lw 2,\
"< awk '{print FNR,$0}' data/Space_Train_term_features.txt" using 1:($2+(1.0/100000)) title 'Space'  with points lw 2,\
"< awk '{print FNR,$0}' data/Soccer_Train_term_features.txt" using 1:($2+(1.0/100000)) title 'Soccer'  with points lw 2,\
"< awk '{print FNR,$0}' data/Iran_Train_term_features.txt" using 1:($2+(1.0/100000)) title 'Iran Nuclear Deal'  with points lw 2,\
"< awk '{print FNR,$0}' data/Human_Disaster_Train_term_features.txt" using 1:($2+(1.0/100000)) title 'Human Disaster'  with points lw 2,\
"< awk '{print FNR,$0}' data/Cele_death_Train_term_features.txt" using 1:($2+(1.0/100000)) title 'Celebrity Death'  with points lw 2,\
"< awk '{print FNR,$0}' data/Social_issue_Train_term_features.txt" using 1:($2+(1.0/100000)) title 'Social Issues'  with points lw 2,\
"< awk '{print FNR,$0}' data/Natr_Disaster_Train_term_features.txt" using 1:($2+(1.0/100000)) title 'Natural Disasters'  with points lw 2,\
"< awk '{print FNR,$0}' data/Health_Train_term_features.txt" using 1:($2+(1.0/100000)) title 'Epidemics'  with points lw 2,\
"< awk '{print FNR,$0}' data/LGBT_Train_term_features.txt" using 1:($2+(1.0/100000)) title 'LGBT'  with points lw 2


set output "Tennis_features.png"
plot "< awk '{print FNR,$0}' data/Tennis_Train_term_features.txt" using 1:($2+(1.0/100000)) title 'Terms'  with points lw 2,\
"< awk '{print FNR,$0}' data/Tennis_Train_hashtag_features.txt" using 1:($2+(1.0/100000)) title 'Hashtag'  with points lw 2,\
"< awk '{print FNR,$0}' data/Tennis_Train_mention_features.txt" using 1:($2+(1.0/100000)) title 'Mention'  with points lw 2,\
"< awk '{print FNR,$0}' data/Tennis_Train_user_features.txt" using 1:($2+(1.0/100000)) title 'User'  with points lw 2,\
"< awk '{print FNR,$0}' data/Tennis_Train_loc_feature.txt" using 1:($2+(1.0/100000)) title 'Location'  with points lw 2

