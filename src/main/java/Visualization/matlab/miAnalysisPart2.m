path = '/data/MIAnalysis';

fileName = 'ProbsLess_allMI_CountfTtT.csv';
fid = fopen([path '/' fileName]);
out = textscan(fid,'%f','delimiter',',');
probs = out{1};
avgTT = mean(probs);
medianTT = median(probs);
fclose(fid);

fileName = 'ProbsLess_allMI_CountfTtF.csv';
fid = fopen([path '/' fileName]);
out = textscan(fid,'%f','delimiter',',');
probs = out{1};
avgTF = mean(probs);
medianTF = median(probs);
fclose(fid);

fileName = 'ProbsLess_allMI_CountfFtT.csv';
fid = fopen([path '/' fileName]);
out = textscan(fid,'%f','delimiter',',');
probs = out{1};
avgFT = mean(probs);
medianFT = median(probs);
fclose(fid);

fileName = 'ProbsLess_allMI_CountfFtF.csv';
fid = fopen([path '/' fileName]);
out = textscan(fid,'%f','delimiter',',');
probs = out{1};
avgFF = mean(probs);
medianFF = median(probs);
fclose(fid);

avgValuesLess = [avgTT avgTF avgFT avgFF]
medianValuesLess = [medianTT medianTF medianFT medianFF]
