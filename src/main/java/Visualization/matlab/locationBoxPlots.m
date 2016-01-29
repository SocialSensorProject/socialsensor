fName = 'MI';
path = ['../PlotFiles/' fName '/Location/LocationDependent/'];
flagMI = true;
folders=dir(path);
stddevInd = 1;
tp = [];
stdDeviations = zeros(length(folders)-2, 18);

for ij = 1:length(folders)
    figure;
    folder = folders(ij);
    if (strcmp(folder.name(1),'.'))
        continue;
    end
    if(strcmp(folder.name,'statusesCount') || strcmp(folder.name, 'MIPlots'))
       %strcmp(folder.name,'followersCount')||strcmp(folder.name,'favoriteCount') ||  || strcmp(folder.name,'friendsCount'))
        continue;
    end
    Files = dir([path folder.name '/']);    
    topics = [];
    ind = 1;topicInd = 1;fileInd=1;
    probs = [];y = [];
    topicName = folder.name;
    for k=1:length(Files)
        fileNames=Files(k).name;
        fileNames
        if (strcmp(fileNames(1),'.') || strcmp(fileNames, 'BoxPlots') || strcmp(fileNames, 'Plots'))
           continue;
        end
        topics{topicInd} = fileNames;        
        fid = fopen([path folder.name '/' fileNames]);
        out = textscan(fid,'%f%f%s%d','delimiter',',');
        %counts(fileInd, :) = out{1};
        probs = [probs; out{2}];
        y = [y; ones(length(out{1}),1)*fileInd];
        fileInd = fileInd + 1;
        st = strsplit(fileNames, '_');
        %tick{fileInd} = st(4)
    end
    
    ax=boxplot(probs, y);
    labels = {'Argentina', 'Brasil', 'Canada', 'Egypt', 'France', 'India', 'Indonesia', 'Italy', 'Japan', 'Mexico', 'Nigeria', 'Philippines', 'South Africa', 'Thailand', 'UK', 'US'};
    xticklabel_rotate(1:16,90,labels,'interpreter','none');
    name1 = strsplit(fileNames, '_');
    %set(ax, 'XTicks', ['Argentina', 'Brasil', 'Canada', 'Egypt', 'France', 'India', 'Indonesia', 'Italy', 'Japan', 'Mexico', 'Nigeria, 'Philippines', 'South Africa', 'Thailand', 'UK', 'US']);
    xlabel('Locations');
    set(gca, 'YScale', 'log');
    ylabel([fName ' (log-scale)']);
    title([fName ' of Locations vs ' topicName]);
    grid on
    print(gcf, [path 'Plots/' name1{1} '_vs_Locations'], '-dpng');
end
