fName = 'From';
path = ['/scratch/PlotFiles/MI/' fName '/'];

flagMI = true;
folders=dir(path);
stddevInd = 1;
tp = [];
stdDeviations = zeros(length(folders)-2, 18);
for ij = 1:length(folders)
    folder = folders(ij);
    if (strcmp(folder.name(1),'.'))
        continue;
    end
    if(strcmp(folder.name,'statusesCount') )
       %strcmp(folder.name,'followersCount')||strcmp(folder.name,'favoriteCount') ||  || strcmp(folder.name,'friendsCount'))
        continue;
    end
    Files = dir([path folder.name '/']);
    q = zeros(6, 3*6);
    figure;
    topics = [];
    ind = 1;topicInd = 1;
    for k=1:length(Files)
        fileNames=Files(k).name;
        fileNames
        if (strcmp(fileNames(1),'.') || strcmp(fileNames, 'BoxPlots') || strcmp(fileNames, 'Plots'))
           continue;
        end
        topics{topicInd} = fileNames;        
        fid = fopen([path folder.name '/' fileNames]);
        out = textscan(fid,'%f%f%s%d','delimiter',',');
        counts = out{1};
        probs = out{2};
        out = [];
        randIndices = randperm(length(probs), min(length(probs), 1e6));
        probs = probs(randIndices);
        counts = counts(randIndices);
        max(counts)
        %[N,edges] = histcounts(out{2},edges);
        edges = [1e2 1e3 1e4 1e5 1e6 1e8];
%         for pk=1:length(edges)
%             data(pk) = 0;
%         end
        x = []; y = []; ind = 1;ind2=1;
        for i=1:length(edges)
            if i==1
                x = [x; probs(counts <= edges(i) & counts > 0)];
                y = [y; edges(i*ones(1, sum(counts <= edges(i) & counts > 0)))'];
                pk = i*ones(1, sum(counts <= edges(i) & counts > 0));
                %data(pk) = [data(pk); x];
                q(topicInd, ind:ind+2) = quantile(probs(counts <= edges(i) & counts > 0), [0.25 0.5 0.75]);
            else
                x = [x; probs(counts <= edges(i) & counts > edges(i-1))];
                y = [y; edges(i*ones(1, sum(counts <= edges(i) & counts > edges(i-1))))'];
                pk = i*ones(1, sum(counts <= edges(i) & counts > edges(i-1)));
                %data(pk) = [data(pk); x];
                q(topicInd, ind:ind+2) = quantile(probs(counts <= edges(i) & counts > edges(i-1)), [0.25 0.5 0.75]);
            end
            if(length(pk) > 0)
                data{ind2} = [log10(x+1e-30) repmat(find(edges == y(1)), length(x), 1)];
                ind2 = ind2+1;
            end
            ind = ind+3;
        end
        topicInd=topicInd+1;
        boxplot(x, y);
        if(flagMI)
            set(gca,'YScale','log');
        end
        legend('off');
        names = strsplit(fileNames, '_');
        names2 = strsplit(char(names(3)), '.');
        titleName = ['BoxPlot ', char(names(1)), ' ', char(names(2)), ' ',char(names2(1))];
        tName = [char(names(1)), ' ', char(names(2)), ' ',char(names2(1)), '_log_Plot'];
        title(titleName);
        xlabel(names(2));
        if(strcmp(names(3),'MI') == 1)
            set(gca,'YScale','log');
            ylabel(['log-' char(names2(1))]);
        else
            ylabel(char(names2(1)));
        end
        mkdir([path,folder.name, '/BoxPlots/']);
        mkdir([path,folder.name, '/Plots/'])
        print(gcf,[path,folder.name, '/BoxPlots/',tName], '-dpng');
        %hold all;
        PlotWithLabelsLogScale([path folder.name '/'],fileNames, out);
    end
    q(isnan(q)) = 0;
    stdDeviations(stddevInd, :) = std(q);
    tp{stddevInd} = folder.name;
    stddevInd = stddevInd+1;
    save(['quartiles_' fName '_' folder.name], 'q');
    %save 5;
end
q
