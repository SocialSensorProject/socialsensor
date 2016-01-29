path = '/data/ClusterData/input/monthNumbers_csv/';
fName = 'monthNumbers.csv';
fid = fopen([path '/' fName]);
out = textscan(fid,'%d','delimiter',',');
probs = out{1};

[X Y] = histc(probs, 1:24);
plot(X, 1:24);