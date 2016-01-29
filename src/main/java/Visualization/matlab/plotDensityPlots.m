fName = 'From';
path = ['/scratch/PlotFiles/MI/' fName '/'];
plotInd = 1;
%ha = tight_subplot(1,4,[.06 .03],[.06 .04]);
folders=dir(path);
for ij = 1:length(folders)
    folder = folders(ij)
    if (strcmp(folder.name(1),'.') || strcmp(folder.name,'tweetCount')  || strcmp(folder.name,'statusesCount') || strcmp(folder.name, 'LocationDependent') || strcmp(folder.name, 'MI_vs_Topics.png'))
        continue;
    end
    Files = dir([path folder.name '/']);
    %figure;
    topics = [];
    ind = 1;topicInd = 1;
    for k=1:length(Files)
        fileNames=Files(k).name
        if (strcmp(fileNames(1),'.') || strcmp(fileNames, 'BoxPlots') || strcmp(fileNames, 'Plots') || strcmp(fileNames, 'MIPlots'))
           continue;
        end
        if(~strcmp(fileNames(1:min(8,length(fileNames))), 'irandeal'))
            continue;
        end
        fileNames
       
        topics{topicInd} = fileNames;        
        fid = fopen([path folder.name '/' fileNames]);
        out = textscan(fid,'%f%f%s%d','delimiter',',');
        counts = out{1};
        probs = out{2};
        out = [];
        len = length(probs);
        randIndices = randperm(len, min(len, 1e6));
        probs = probs(randIndices);
        counts = counts(randIndices);
        max(counts)
        names = strsplit(fileNames, '_');
        names2 = strsplit(char(names(3)), '.');
        names3 = strsplit(char(names(1)), '-');
        titleStr = [char(names3(2)), '-', char(names(2))];
        xlabelStr = ['' char(names(2))];
        ylabelStr = ['' char(names2(1))];
        %axes(ha(plotInd));
        %ha = subplot(2,5, plotInd);
        plotInd = plotInd+1;
        figure;
        dscatter(real(log10(counts+1e-30)), real(log10(probs+1e-30)), [path folder.name '/'], titleStr, titleStr, xlabelStr, ylabelStr, 'plottype','scatter', 'BINS', [200 200]); 
    end
end
pubmode('on');
set(h,'PaperOrientation','landscape');
set(h,'PaperUnits','normalized');
set(h,'PaperPosition', [0 0 1 1]);
print(gcf, '-dpdf', 'test3.pdf');