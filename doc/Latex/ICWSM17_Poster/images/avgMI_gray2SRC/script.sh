clear
reset
#set tic scale 0
set xtics nomirror rotate by -45
set xtics left offset 0,0
unset colorbox
set size 1,0.8
# Color runs from white to green
set palette defined ( 0 "white", 2 "#7F6BFF" )
set cbrange [0:150]
#set cblabel "Score"
#unset cbtics
set xtics   ("Tennis" 0.0, "Space" 1.0, "Soccer" 2.0, "Iran Deal" 3.0, "Human Disaster" 4.0, "Celebrity Death" 5.0, "Social Issues" 6.0, "Natural Disaster" 7.0, "Epidemics" 8.0, "LGBT" 9.0, "Mean" 10.0) font "Courier-Bold,17"
set ytics   ("Term" 0, "Location" 1, "From" 2, "Hashtag" 3, "Mention" 4) font "Courier-Bold,17"
set xrange [-0.5:10.5]
set yrange [-0.5:4.5]
set view map
#set margin 4,0,7,0
set bmargin 11
set lmargin 13
set term postscript eps enhanced color  font "Courier,14"
set output "avgMI_gray2.eps"
plot 'matrix.txt' using  1:2:3 matrix with image, \
     'matrix.txt'  using 1:2:(sprintf("%g",$3)) matrix  with labels notitle
     