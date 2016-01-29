path = '/scratch/PlotFiles/MI/From/favoriteCount';
fileName = 'irandeal-From_favoriteCount_MI.csv';
fid = fopen([path '/' fileName]);
out = textscan(fid,'%f%f%s%d','delimiter',',');
counts = out{1};
probs = out{2};
names = out{3};
out = [];
len = length(probs);
edges = [1e-15 1e-14 1e-13 1e-12 1e-11 2e-11 3e-11 4e-11 5e-11 6e-11 7e-11 8e-11 9e-11 1e-10 2e-10 3e-10 4e-10 5e-10 6e-10 7e-10 8e-10 9e-10 1e-9 2e-9 3e-9 4e-9 5e-9 6e-9 7e-9 8e-9 9e-9 1e-8 5e-8 1e-7 5e-7 1e-6];
[n m] = histcounts(probs, edges);


inds = find(probs < 8e-11);
randIndices = inds(randperm(length(inds), min(length(inds), 1e6)));
probs = probs(randIndices);
counts = counts(randIndices);
namesLess8E11 = names(randIndices);
fid = fopen('namesLess8E11.csv','w');
for i =1:length(namesLess8E11)
    fprintf(fid,'%s\n',namesLess8E11{i})
end
fclose(fid);

inds = find(probs > 2e-9);
namesGreater2E9 = names(inds);
fid = fopen('namesGreater2E9.csv','w');
for i =1:length(namesGreater2E9)
    fprintf(fid,'%s\n',namesGreater2E9{i})
end
fclose(fid);
randIndices = randperm(len, min(len, 1e6));
probs = probs(randIndices);
counts = counts(randIndices);
%scatter(counts, probs);
%set(gca,'YScale','log');
%set(gca,'XScale','log');
%probs = [];