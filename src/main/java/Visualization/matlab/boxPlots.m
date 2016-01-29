path = '/data/PlotFiles/CP/From/';
Files=dir([path,'*.*']);
q = zeros(length(Files), 8, 5);
for k=1:length(Files)
    fileNames=Files(k).name;
    if (strcmp(fileNames(1),'.'))
       continue;
    end
    fid = fopen([path fileNames]);
    out = textscan(fid,'%f%f%s%d','delimiter',',');
    counts = out{1};
    probs = out{2};
    max(counts)
    %[N,edges] = histcounts(out{2},edges);
    edges = [10 100 1e3 1e4 1e5 1e6 1e7 1e8];
    x = []; y = [];
    for i=1:length(edges)
        if i==1
            x = [x; probs(counts <= edges(i) & counts > 0)];
            y = [y; edges(i*ones(1, sum(counts <= edges(i) & counts > 0)))'];
            q(k, i, :) = quantile(probs(counts <= edges(i) & counts > 0), [0.03 0.25 0.5 0.75 0.97]);
        else
            x = [x; probs(counts <= edges(i) & counts > edges(i-1))];
            y = [y; edges(i*ones(1, sum(counts <= edges(i) & counts > edges(i-1))))'];
            q(k, i, :) = quantile(probs(counts <= edges(i) & counts > edges(i-1)), [0.03 0.25 0.5 0.75 0.97]);
        end
    end
    boxplot(x, y);
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
    print(gcf,[path,'BoxPlots/',tName], '-dpng');
end
q