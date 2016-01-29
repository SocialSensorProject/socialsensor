fName = 'MI';
featureNames = { 'Hashtag', 'From', 'Location', 'Term', 'Mention'};
topicsLength = 10;
maxSize = 1e6;
table1 = zeros(length(featureNames), topicsLength+2);
for featureInd = 1:length(featureNames)
    featureName = featureNames{featureInd};
    path = '/data/FeatureAnalysis/';
    folders=dir(path);
    topics = [];
    ind = 1;topicInd = 1;fileInd=1;
    probs = [];y = [];
    figure;
    for ij = 1:length(folders)%each topic   
        folder = folders(ij);
        topicName = folder.name;  
        topics{topicInd} = topicName;    
        if (strcmp(folder.name(1),'.'))
            continue;
        end
        if(strcmp(folder.name,'PlotsDec2') || strcmp(folder.name, 'MIPlots') || strcmp(folder.name, 'LocationsMI'))
            continue;
        end
        
        fileName = [path topicName '/' fName '/' featureName '/' featureName '1.csv'];        
        fid = fopen(fileName);
        out = textscan(fid,'%s%f','delimiter',',');        
        XX = out{2};
        fileName
        out = [];
        len = length(XX)
        %randIndices = randperm(len, min(maxSize, len));        
        %probs = [probs; XX(randIndices)];
        %y = [y; ones(min(maxSize, len),1)*topicInd];
        st = [topicName '_' featureName];
        table1(featureInd, topicInd) = mean(XX);
        topicInd = topicInd + 1;
        fclose(fid);
    end    
    table1(featureInd, topicsLength+1) = mean(table1(featureInd, 1:topicsLength));
    table1(featureInd, topicsLength+2) = std(table1(featureInd, 1:topicsLength));
%     ax=boxplot(probs, y);
%     xticklabel_rotate(1:length(topics),45,topics,'interpreter','none');
%     xlabel('Topics');    
%     title([fName ' of ' featureName ' vs Topics']);
%     print(gcf, [path 'PlotsDec/MI_' featureName '_vs_Topics'], '-dpng');
%     set(gca, 'YScale', 'log');
%     axis([0 11 0 10e-3]);
%     %ylabel([fName ' (log-scale)']);    
%     grid on
%     print(gcf, [path 'PlotsDec/LogMI_' featureName '_vs_Topics'], '-dpng');
%     featureName
%     save('TableAvgMIvsTopics', 'table1');
    table1
end
%%
figure;
imagesc(diff);
colormap(hot);
textStrings = num2str(diff(:),'%0.2f');
textStrings = strtrim(cellstr(textStrings));
[x,y] = meshgrid(1:size(diff,2), 1:5);
hStrings = text(x(:),y(:),textStrings(:),'HorizontalAlignment','center');
for i = 1:length(hStrings)
    hStrings(i).FontSize = 6; 
    hStrings(i).Color = 'white';
end
t = {'celebritydeath' ,   'epidemics' ,   'humancauseddisaster'  ,  'irandeal'  ,  'lbgt'  ,  'naturaldisaster'  ,  'soccor'  ,  'socialissues', 'space', 'tennis', 'mean', 'std'};
xticklabel_rotate(1:length(topics)+1,45,t,'interpreter','none');
ylables = {'Hashtag', 'Location', 'Term', 'From', 'Mention'};
set(gca,'YTick',ylables);