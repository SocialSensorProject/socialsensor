
clear
reset
#set tic scale 0
set xtics nomirror rotate by -45
set xtics left offset 0,0
unset colorbox
set size 1,0.8
# Color runs from white to green
set palette defined ( 0 "white", 2 "#7F6BFF" )
set cbrange [0:26]
#set cblabel "Score"
#unset cbtics
set datafile missing "-"
set parametric
set trange [-1:11]
const1=9.5
set xtics   ("Tennis" 0.0, "Space" 1.0, "Soccer" 2.0, "Iran Deal" 3.0, "Human Disaster" 4.0, "Celebrity Death" 5.0, "Social Issues" 6.0, "Natural Disaster" 7.0, "Epidemics" 8.0, "LGBT" 9.0, "Mean" 10.0) font "Courier-Bold,17"
set ytics   ("Mean" 0, "Term" 1, "Location" 2, "From" 3, "Hashtag" 4, "Mention" 5) font "Courier-Bold,17"
set xrange [-0.5:10.5]
set yrange [-0.5:4.5]
set view map
#f(x)=1

const2=0.5



#set margin 4,0,7,0
set bmargin 11
set lmargin 13
set term postscript eps enhanced color  font "Courier,14"
set output "plots/avgMI_gray.eps"
plot 'data/matrix.txt' using  1:2:3 matrix with image, \
     'data/matrix.txt'  using 1:2:(sprintf("%g",$3)) matrix  with labels  notitle,\
      const1,t lc 'black' lw 2 lt 2  notitle,\
      t,const2 lc 'black' lw 2 lt 2  notitle





reset
clear
#set border 2 front linetype -1 linewidth 1.000
#set boxwidth 0.5 absolute
set style fill   solid 1 border lt -1
set key
set grid
set yrange [10**-12:10**-2] 
set xrange [0:11] 
#set logscale y
set size 0.8,0.6
#set pointsize 0.5
set border 4095
set  logscale y
set datafile missing "NaN"
set format y "10^{%L}"


set style data boxplot 
set style boxplot 

set xtics ("Tennis" 1,"Space" 2,"Soccer" 3,"Iran Deal" 4,"Human Disaster" 5,"Celebrity Death" 6,"Social Issues" 7,"Natural Disasters" 8,"Epidemics" 9,"LGBT" 10)  rotate by 45 right
set term postscript eps enhanced color "Courier,17"



set ylabel "Mutual Information" font "Courier,16"
set output "plots/term_features.eps" 
plot "< head -n 100000 data/Tennis_Dataset_term_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Space_Dataset_term_features.txt" using (2):($2+rand(0)*1E-20) every 20  notitle,\
"< head -n 100000 data/Soccer_Dataset_term_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Iran_Dataset_term_features.txt" using (4):($2+rand(0)*1E-20) every 20  notitle,\
"< head -n 100000 data/Human_Disaster_Dataset_term_features.txt" using (5):($2+rand(0)*1E-20) every 20  notitle, "< head -n 100000 data/Cele_death_Dataset_term_features.txt" using (6):($2+rand(0)*1E-20) every 20  notitle, \
"< head -n 100000 data/Social_issue_Dataset_term_features.txt" using (7):($2+rand(0)*1E-20) every 20  notitle, "< head -n 100000 data/Natr_Disaster_Dataset_term_features.txt" using (8):($2+rand(0)*1E-20) every 20  notitle lc 'orange', \
"< head -n 100000 data/Health_Dataset_term_features.txt" using (9):($2+rand(0)*1E-20) every 20  notitle lc 'cyan', "< head -n 100000 data/LGBT_Dataset_term_features.txt" using (10):($2+rand(0)*1E-20) every 20  notitle lc 'grey'


set ylabel "Mutual Information" font "Courier,16"
set output "plots/hashtag_features.eps" 
plot "< head -n 100000 data/Tennis_Dataset_hashtag_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Space_Dataset_hashtag_features.txt" using (2):($2+rand(0)*1E-20) every 20  notitle,\
"< head -n 100000 data/Soccer_Dataset_hashtag_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Iran_Dataset_hashtag_features.txt" using (4):($2+rand(0)*1E-20) every 20  notitle,\
"< head -n 100000 data/Human_Disaster_Dataset_hashtag_features.txt" using (5):($2+rand(0)*1E-20) every 20  notitle, "< head -n 100000 data/Cele_death_Dataset_hashtag_features.txt" using (6):($2+rand(0)*1E-20) every 20  notitle, \
"< head -n 100000 data/Social_issue_Dataset_hashtag_features.txt" using (7):($2+rand(0)*1E-20) every 20  notitle, "< head -n 100000 data/Natr_Disaster_Dataset_hashtag_features.txt" using (8):($2+rand(0)*1E-20) every 20  notitle lc 'orange', \
"< head -n 100000 data/Health_Dataset_hashtag_features.txt" using (9):($2+rand(0)*1E-20) every 20  notitle lc 'cyan', "< head -n 100000 data/LGBT_Dataset_hashtag_features.txt" using (10):($2+rand(0)*1E-20) every 20  notitle lc 'grey'

set ylabel "Mutual Information" font "Courier,16"
set output "plots/loc_features.eps" 
plot "< head -n 100000 data/Tennis_Dataset_loc_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Space_Dataset_loc_features.txt" using (2):($2+rand(0)*1E-20) every 20  notitle,\
"< head -n 100000 data/Soccer_Dataset_loc_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Iran_Dataset_loc_features.txt" using (4):($2+rand(0)*1E-20) every 20  notitle,\
"< head -n 100000 data/Human_Disaster_Dataset_loc_features.txt" using (5):($2+rand(0)*1E-20) every 20  notitle, "< head -n 100000 data/Cele_death_Dataset_loc_features.txt" using (6):($2+rand(0)*1E-20) every 20  notitle, \
"< head -n 100000 data/Social_issue_Dataset_loc_features.txt" using (7):($2+rand(0)*1E-20) every 20  notitle, "< head -n 100000 data/Natr_Disaster_Dataset_loc_features.txt" using (8):($2+rand(0)*1E-20) every 20  notitle lc 'orange', \
"< head -n 100000 data/Health_Dataset_loc_features.txt" using (9):($2+rand(0)*1E-20) every 20  notitle lc 'cyan', "< head -n 100000 data/LGBT_Dataset_loc_features.txt" using (10):($2+rand(0)*1E-20) every 20  notitle lc 'grey'

set ylabel "Mutual Information" font "Courier,16"
set output "plots/mention_features.eps" 
plot "< head -n 100000 data/Tennis_Dataset_mention_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Space_Dataset_mention_features.txt" using (2):($2+rand(0)*1E-20) every 20  notitle,\
"< head -n 100000 data/Soccer_Dataset_mention_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Iran_Dataset_mention_features.txt" using (4):($2+rand(0)*1E-20) every 20  notitle,\
"< head -n 100000 data/Human_Disaster_Dataset_mention_features.txt" using (5):($2+rand(0)*1E-20) every 20  notitle, "< head -n 100000 data/Cele_death_Dataset_mention_features.txt" using (6):($2+rand(0)*1E-20) every 20  notitle, \
"< head -n 100000 data/Social_issue_Dataset_mention_features.txt" using (7):($2+rand(0)*1E-20) every 20  notitle, "< head -n 100000 data/Natr_Disaster_Dataset_mention_features.txt" using (8):($2+rand(0)*1E-20) every 20  notitle lc 'orange', \
"< head -n 100000 data/Health_Dataset_mention_features.txt" using (9):($2+rand(0)*1E-20) every 20  notitle lc 'cyan', "< head -n 100000 data/LGBT_Dataset_mention_features.txt" using (10):($2+rand(0)*1E-20) every 20  notitle lc 'grey'

set ylabel "Mutual Information" font "Courier,16"
set output "plots/user_features.eps" 
plot "< head -n 100000 data/Tennis_Dataset_user_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Space_Dataset_user_features.txt" using (2):($2+rand(0)*1E-20) every 20  notitle,\
"< head -n 100000 data/Soccer_Dataset_user_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Iran_Dataset_user_features.txt" using (4):($2+rand(0)*1E-20) every 20  notitle,\
"< head -n 100000 data/Human_Disaster_Dataset_user_features.txt" using (5):($2+rand(0)*1E-20) every 20  notitle, "< head -n 100000 data/Cele_death_Dataset_user_features.txt" using (6):($2+rand(0)*1E-20) every 20  notitle, \
"< head -n 100000 data/Social_issue_Dataset_user_features.txt" using (7):($2+rand(0)*1E-20) every 20  notitle, "< head -n 100000 data/Natr_Disaster_Dataset_user_features.txt" using (8):($2+rand(0)*1E-20) every 20  notitle lc 'orange', \
"< head -n 100000 data/Health_Dataset_user_features.txt" using (9):($2+rand(0)*1E-20) every 20  notitle lc 'cyan', "< head -n 100000 data/LGBT_Dataset_user_features.txt" using (10):($2+rand(0)*1E-20) every 20  notitle lc 'grey'












reset
clear
#set border 2 front linetype -1 linewidth 1.000
#set boxwidth 0.5 absolute
set style fill   solid 1 border lt -1
set key
set grid
set yrange [10**-12:10**-2] 
set xrange [0.5:5.5] 
#set logscale y
set size 0.5,0.6
#set pointsize 0.5
set border 4095
set  logscale y
set datafile missing "NaN"
set format y "10^{%L}"


set style data boxplot 
set style boxplot 

set xtics ("Term" 1,"Location" 2,"Hashtag" 3,"Mention" 4,"User" 5)  rotate by 45 right
set term postscript eps enhanced color "Courier,19"


set ylabel "Mutual Information" font "Courier,16"
set output "plots/Tennis_features.eps" 
plot "< head -n 100000 data/Tennis_Dataset_term_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Tennis_Dataset_loc_features.txt" using (2):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Tennis_Dataset_hashtag_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Tennis_Dataset_mention_features.txt" using (4):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Tennis_Dataset_user_features.txt" using (5):($2+rand(0)*1E-20)  every 20  notitle


set output "plots/Space_features.eps" 
plot "< head -n 100000 data/Space_Dataset_term_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Space_Dataset_loc_features.txt" using (2):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Space_Dataset_hashtag_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Space_Dataset_mention_features.txt" using (4):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Space_Dataset_user_features.txt" using (5):($2+rand(0)*1E-20)  every 20  notitle


set output "plots/Soccer_features.eps" 
plot "< head -n 100000 data/Soccer_Dataset_term_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Soccer_Dataset_loc_features.txt" using (2):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Soccer_Dataset_hashtag_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Soccer_Dataset_mention_features.txt" using (4):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Soccer_Dataset_user_features.txt" using (5):($2+rand(0)*1E-20)  every 20  notitle

set output "plots/Iran_features.eps" 
plot "< head -n 100000 data/Iran_Dataset_term_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Iran_Dataset_loc_features.txt" using (2):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Iran_Dataset_hashtag_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Iran_Dataset_mention_features.txt" using (4):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Iran_Dataset_user_features.txt" using (5):($2+rand(0)*1E-20)  every 20  notitle


set output "plots/Human_Disaster_features.eps" 
plot "< head -n 100000 data/Human_Disaster_Dataset_term_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Human_Disaster_Dataset_loc_features.txt" using (2):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Human_Disaster_Dataset_hashtag_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Human_Disaster_Dataset_mention_features.txt" using (4):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Human_Disaster_Dataset_user_features.txt" using (5):($2+rand(0)*1E-20)  every 20  notitle

set output "plots/Cele_death_features.eps" 
plot "< head -n 100000 data/Cele_death_Dataset_term_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Cele_death_Dataset_loc_features.txt" using (2):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Cele_death_Dataset_hashtag_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Cele_death_Dataset_mention_features.txt" using (4):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Cele_death_Dataset_user_features.txt" using (5):($2+rand(0)*1E-20)  every 20  notitle

set output "plots/Social_issue_features.eps" 
plot "< head -n 100000 data/Social_issue_Dataset_term_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Social_issue_Dataset_loc_features.txt" using (2):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Social_issue_Dataset_hashtag_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Social_issue_Dataset_mention_features.txt" using (4):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Social_issue_Dataset_user_features.txt" using (5):($2+rand(0)*1E-20)  every 20  notitle

set output "plots/Natr_Disaster_features.eps" 
plot "< head -n 100000 data/Natr_Disaster_Dataset_term_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Natr_Disaster_Dataset_loc_features.txt" using (2):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Natr_Disaster_Dataset_hashtag_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Natr_Disaster_Dataset_mention_features.txt" using (4):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Natr_Disaster_Dataset_user_features.txt" using (5):($2+rand(0)*1E-20)  every 20  notitle

set output "plots/Health_features.eps" 
plot "< head -n 100000 data/Health_Dataset_term_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Health_Dataset_loc_features.txt" using (2):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Health_Dataset_hashtag_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/Health_Dataset_mention_features.txt" using (4):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/Health_Dataset_user_features.txt" using (5):($2+rand(0)*1E-20)  every 20  notitle

set output "plots/LGBT_features.eps" 
plot "< head -n 100000 data/LGBT_Dataset_term_features.txt" using (1):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/LGBT_Dataset_loc_features.txt" using (2):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/LGBT_Dataset_hashtag_features.txt" using (3):($2+rand(0)*1E-20)  every 20  notitle, "< head -n 100000 data/LGBT_Dataset_mention_features.txt" using (4):($2+rand(0)*1E-20)  every 20  notitle,\
"< head -n 100000 data/LGBT_Dataset_user_features.txt" using (5):($2+rand(0)*1E-20)  every 20  notitle



