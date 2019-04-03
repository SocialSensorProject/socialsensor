reset
clear
set key
set grid

set size 0.6,0.6
set border 4095

set yrange [0:1]
set xrange [0:10] 

f1(x) = a1*x + b1
f2(x) = a2*x + b2


f1(x) = a1*x + b1
f2(x) = a2*x + b2

set ylabel "Average Precision" font "Courier,22"
set xlabel "Time" font "Courier,22"


set term postscript eps enhanced color "Courier,17"

#  Cele_death
set output "plots/Cele_death.eps" 
fit f1(x) 'data/Cele_death1.txt' u 1:2 via a1, b1
fit f2(x) 'data/Cele_death2.txt' u 1:2 via a2, b2

plot  'data/Cele_death1.txt' u 1:2 notitle  with linespoints lw 3 lc 1 , f1(x) title 'No training #' lw 3 lc 1,\
  'data/Cele_death1.txt' u 1:2  notitle with linespoints lw 3 lc 2 , f2(x) title 'Keep training #'lw 3 lc 2


 #  Health
set output "plots/Health.eps" 
fit f1(x) 'data/Health1.txt' u 1:2 via a1, b1
fit f2(x) 'data/Health2.txt' u 1:2 via a2, b2

plot  'data/Health1.txt' u 1:2  notitle with linespoints lw 3 lc 1 , f1(x) title 'No training #' lw 3 lc 1,\
  'data/Health2.txt' u 1:2  notitle with linespoints lw 3 lc 2 , f2(x) title 'Keep training #'lw 3 lc 2
#  Human_Disaster
set output "plots/Human_Disaster.eps" 
fit f1(x) 'data/Human_Disaster1.txt' u 1:2 via a1, b1
fit f2(x) 'data/Human_Disaster2.txt' u 1:2 via a2, b2

plot  'data/Human_Disaster1.txt' u 1:2  notitle with linespoints lw 3 lc 1 , f1(x) title 'No training #' lw 3 lc 1,\
  'data/Human_Disaster2.txt' u 1:2  notitle with linespoints lw 3 lc 2 , f2(x) title 'Keep training #'lw 3 lc 2
#  Iran
set output "plots/Iran.eps" 
fit f1(x) 'data/Iran1.txt' u 1:2 via a1, b1
fit f2(x) 'data/Iran2.txt' u 1:2 via a2, b2

plot  'data/Iran1.txt' u 1:2  notitle with linespoints lw 3 lc 1 , f1(x) title 'No training #' lw 3 lc 1,\
  'data/Iran2.txt' u 1:2 notitle with linespoints lw 3 lc 2 , f2(x) title 'Keep training #'lw 3 lc 2
#  LGBT
set output "plots/LGBT.eps" 
fit f1(x) 'data/LGBT1.txt' u 1:2 via a1, b1
fit f2(x) 'data/LGBT2.txt' u 1:2 via a2, b2

plot  'data/LGBT1.txt' u 1:2  notitle with linespoints lw 3 lc 1 , f1(x) title 'No training #' lw 3 lc 1,\
  'data/LGBT2.txt' u 1:2 notitle with linespoints lw 3 lc 2 , f2(x) title 'Keep training #'lw 3 lc 2
#  Natr_Disaster
set output "plots/Natr_Disaster.eps" 
fit f1(x) 'data/Natr_Disaster1.txt' u 1:2 via a1, b1
fit f2(x) 'data/Natr_Disaster2.txt' u 1:2 via a2, b2

plot  'data/Natr_Disaster1.txt' u 1:2 notitle  with linespoints lw 3 lc 1 , f1(x) title 'No training #' lw 3 lc 1,\
  'data/Natr_Disaster2.txt' u 1:2  notitle with linespoints lw 3 lc 2 , f2(x) title 'Keep training #'lw 3 lc 2
#  Soccer
set output "plots/Soccer.eps" 
fit f1(x) 'data/Soccer1.txt' u 1:2 via a1, b1
fit f2(x) 'data/Soccer2.txt' u 1:2 via a2, b2

plot  'data/Soccer1.txt' u 1:2  notitle with linespoints lw 3 lc 1 , f1(x) title 'No training #' lw 3 lc 1,\
  'data/Soccer2.txt' u 1:2  notitle with linespoints lw 3 lc 2 , f2(x) title 'Keep training #'lw 3 lc 2
#  Social_issue
set output "plots/Social_issue.eps" 
fit f1(x) 'data/Social_issue1.txt' u 1:2 via a1, b1
fit f2(x) 'data/Social_issue2.txt' u 1:2 via a2, b2

plot  'data/Social_issue1.txt' u 1:2  notitle with linespoints lw 3 lc 1 , f1(x) title 'No training #' lw 3 lc 1,\
  'data/Social_issue2.txt' u 1:2  notitle with linespoints lw 3 lc 2 , f2(x) title 'Keep training #'lw 3 lc 2
#  Space
set output "plots/Space.eps" 
fit f1(x) 'data/Space1.txt' u 1:2 via a1, b1
fit f2(x) 'data/Space2.txt' u 1:2 via a2, b2

plot  'data/Space1.txt' u 1:2  notitle with linespoints lw 3 lc 1 , f1(x) title 'No training #' lw 3 lc 1,\
  'data/Space2.txt' u 1:2  notitle with linespoints lw 3 lc 2 , f2(x) title 'Keep training #'lw 3 lc 2
#  Tennis
set output "plots/Tennis.eps" 
fit f1(x) 'data/Tennis1.txt' u 1:2 via a1, b1
fit f2(x) 'data/Tennis2.txt' u 1:2 via a2, b2

plot  'data/Tennis1.txt' u 1:2  notitle with linespoints lw 3 lc 1 , f1(x) title 'No training #' lw 3 lc 1,\
  'data/Tennis2.txt' u 1:2  notitle with linespoints lw 3 lc 2 , f2(x) title 'Keep training #'lw 3 lc 2

################
reset
clear
set key
set grid

set size 0.6,0.6
set border 4095

set yrange [0.2:0.7]
set xrange [1:7] 


unset key
set xtics ("50 days" 1, "100 days" 2, "150 days" 3, "200 days" 4, "250 days" 5, "300 days" 6, "350 days" 7, "400 days" 8, "450 days" 9, "500 days" 10 ) rotate by 45 right

f1(x) = a1*x + b1
f2(x) = a2*x + b2

set xlabel "Time" font "Courier,22"


set term postscript eps enhanced color "Courier,17"


#  Average AP
set ylabel "Average Precision" font "Courier,22"

fit f1(x) 'data/average_topics1.txt' u 1:2 via a1, b1
fit f2(x) 'data/average_topics2.txt' u 1:2 via a2, b2

set output "plots/average_topics_ap.eps" 
plot  'data/average_topics1.txt' u 1:2  notitle with linespoints lw 3 lc 1 ,\
 "data/average_topics1.txt" using 1:2:3 title 'No training #'  with  errorbars pt -1 lw 1 ps 1.95 lc 1,\
 f1(x) notitle lt 'dashed' lw 3 lc 1,\
'data/average_topics2.txt' u 1:2  notitle with linespoints lw 3 lc 2 ,\
"data/average_topics2.txt" using 1:2:3 title 'Keep training #'  with  errorbars pt -1 lw 1 ps 1.95 lc 2,\
f2(x) notitle lt 'dashed' lw 3 lc 2



#f1(x) title 'No training #' lw 3 lc 1,\

#f2(x) title 'Keep training #'lw 3 lc 2




#  Average P@10
set yrange [0.1:0.9]

set ylabel "Precision at 10" font "Courier,22"
fit f1(x) 'data/average_topics1.txt' u 1:4 via a1, b1
fit f2(x) 'data/average_topics2.txt' u 1:4 via a2, b2


set output "plots/average_topics_p10.eps" 
plot  'data/average_topics1.txt' u 1:4  notitle with linespoints lw 3 lc 1 ,\
"data/average_topics1.txt" using 1:4:5 title 'No training #'   with  errorbars pt -1 lw 1 ps 1.95 lc 1,\
f1(x) notitle lt 'dashed' lw 3 lc 1,\
'data/average_topics2.txt' u 1:4  notitle with linespoints lw 3 lc 2 ,\
"data/average_topics2.txt" using 1:4:5 title 'Keep training #'   with  errorbars pt -1 lw 1 ps 1.95 lc 2,\
f2(x) notitle lt 'dashed' lw 3 lc 2



#  Average P@100
set ylabel "Precision at 100" font "Courier,22"
fit f1(x) 'data/average_topics1.txt' u 1:6 via a1, b1
fit f2(x) 'data/average_topics2.txt' u 1:6 via a2, b2

set output "plots/average_topics_p100.eps" 
plot  'data/average_topics1.txt' u 1:6  notitle with linespoints lw 3 lc 1 ,\
"data/average_topics1.txt" using 1:6:7 title 'No training #'   with  errorbars pt -1 lw 1 ps 1.95 lc 1,\
f1(x) notitle lt 'dashed' lw 3 lc 1,\
'data/average_topics2.txt' u 1:6  notitle with linespoints lw 3 lc 2 ,\
"data/average_topics2.txt" using 1:6:7 title 'Keep training #'   with  errorbars pt -1 lw 1 ps 1.95 lc 2,\
f2(x) notitle lt 'dashed' lw 3 lc 2


#  Average P@1000
set yrange [0.0:0.3]

set ylabel "Precision at 1000" font "Courier,22"
fit f1(x) 'data/average_topics1.txt' u 1:8 via a1, b1
fit f2(x) 'data/average_topics2.txt' u 1:8 via a2, b2

set output "plots/average_topics_p1000.eps" 
plot  'data/average_topics1.txt' u 1:8  notitle with linespoints lw 3 lc 1 ,\
"data/average_topics1.txt" using 1:8:9 title 'No training #'   with  errorbars pt -1 lw 1 ps 1.95 lc 1,\
f1(x) notitle lt 'dashed' lw 3 lc 1,\
'data/average_topics2.txt' u 1:8  notitle with linespoints lw 3 lc 2 ,\
"data/average_topics2.txt" using 1:8:9 title 'Keep training #'   with  errorbars pt -1 lw 1 ps 1.95 lc 2,\
f2(x) notitle lt 'dashed' lw 3 lc 2



######
### OVERALL HISTO
###
clear
reset
set style fill   solid 1 border lt -1
set size 0.6,0.5
set grid

#set yrange [0:1]
#set xrange [-1:1]
unset key #outside center top horizontal
#set style data histograms errorbars
set style histogram 
set bars 2 linewidth 5
set style data histograms 

#set xtics   ("Sreda" 0.00000)
#set offset -0.5,-0.5,0,0
#set ytics font "Courier,22"
set xtics ("No\n training #" 0,"Keep\n training #" 0.25) font "Courier,18"
set ylabel "Average Precision" font "Courier,20"
set offset -0.85,-0.6,0,0
set boxwidth 0.5
set bmargin 3
#set lmargin 4

#  Cele_death
set yrange [0.7:1]
set term postscript eps enhanced color "Courier,20"
set output "plots/Cele_death_average.eps" 
plot 'data/Cele_death_average.txt' using 1 lw 2 lc 2 fs pattern 1,\
 '' u 2 lw 2 lc 'red' fs pattern 2
 #  Health
set yrange [0.35:0.45]
set term postscript eps enhanced color "Courier,20"
set output "plots/Health_average.eps" 
plot 'data/Health_average.txt' using 1 lw 2 lc 2 fs pattern 1,\
 '' u 2 lw 2 lc 'red' fs pattern 2
#  Human_Disaster
set yrange [0.63:0.7]
set term postscript eps enhanced color "Courier,20"
set output "plots/Human_Disaster_average.eps" 
plot 'data/Human_Disaster_average.txt' using 1 lw 2 lc 2 fs pattern 1,\
 '' u 2 lw 2 lc 'red' fs pattern 2
#  Iran
set yrange [0.97:0.99]
set term postscript eps enhanced color "Courier,20"
set output "plots/Iran_average.eps" 
plot 'data/Iran_average.txt' using 1 lw 2 lc 2 fs pattern 1,\
 '' u 2 lw 2 lc 'red' fs pattern 2
#  LGBT
set yrange [0.1:0.16]
set term postscript eps enhanced color "Courier,20"
set output "plots/LGBT_average.eps" 
plot 'data/LGBT_average.txt' using 1 lw 2 lc 2 fs pattern 1,\
 '' u 2 lw 2 lc 'red' fs pattern 2
#  Natr_Disaster
set yrange [0.92:0.95]
set term postscript eps enhanced color "Courier,20"
set output "plots/Natr_Disaster_average.eps" 
plot 'data/Natr_Disaster_average.txt' using 1 lw 2 lc 2 fs pattern 1,\
 '' u 2 lw 2 lc 'red' fs pattern 2
#  Soccer
set yrange [0.45:0.51]
set term postscript eps enhanced color "Courier,20"
set output "plots/Soccer_average.eps" 
plot 'data/Soccer_average.txt' using 1 lw 2 lc 2 fs pattern 1,\
 '' u 2 lw 2 lc 'red' fs pattern 2
#  Social_issue
set yrange [0.51:0.58]
set term postscript eps enhanced color "Courier,20"
set output "plots/Social_issue_average.eps" 
plot 'data/Social_issue_average.txt' using 1 lw 2 lc 2 fs pattern 1,\
 '' u 2 lw 2 lc 'red' fs pattern 2
#  Space
set yrange [0.45:0.65]
set term postscript eps enhanced color "Courier,20"
set output "plots/Space_average.eps" 
plot 'data/Space_average.txt' using 1 lw 2 lc 2 fs pattern 1,\
 '' u 2 lw 2 lc 'red' fs pattern 2
#  Tennis
set yrange [0.94:0.96]
set term postscript eps enhanced color "Courier,20"
set output "plots/Tennis_average.eps" 
plot 'data/Tennis_average.txt' using 1 lw 2 lc 2 fs pattern 1,\
 '' u 2 lw 2 lc 'red' fs pattern 2

#average histogram



clear
reset
set yrange [0.3:1]
set style fill   solid 1  border lt -1
set style histogram clustered gap 1 title  offset character 0, 0, 0
set xlabel font "Courier,22"
set size 0.85,0.5
set grid
set style histogram  errorbars linewidth 2
set key vert
unset ylabel
set key center top
set key horiz
set xrang [-0.4:3.4]
set style data histograms
#set style fill pattern border
set term postscript eps enhanced color "Courier,19"
set output "plots/average_topics.eps" 
plot 'data/average_topics_histo.txt' using 2:3:xticlabels(1) ti col lc 1 fs pattern 1, '' u 4:5 ti col lc 2 fs pattern 2



