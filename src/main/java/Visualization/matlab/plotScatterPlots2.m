path = 'Politics/LabeledFiles2/';
Files=dir([path,'*.*']);
fileNumber = 0;
for k=1:length(Files)
   fileNames=Files(k).name;
   if (strcmp(fileNames(1),'.'))
       continue;
   end
   fileNumber = fileNumber + 1;
   if(fileNumber < 5)
       continue;
   fid = fopen([path,fileNames]);
    out = textscan(fid,'%f%f%s%d','delimiter',',');
    x = out{1}; y = out{2}; labels = out{3}; group = out{4};
    fig = figure;
    hScatter = gscatter(x, y, group);
    set(hScatter(1),'Color',[0.2 0.2 0.2]);
    set(hScatter(2),'Color',[0 1 0], 'MarkerSize', 8);%bottom values => GREEN
    set(hScatter(3),'Color',[0 0.75 1], 'MarkerSize', 8);%middle values => BLUE
    set(hScatter(4),'Color',[1 0 0], 'MarkerSize', 8);%top values => RED
    dx = 0.001; dy = 0.001; % displacement so the text does not overlay the data points
    c = [];ind =0;
    for i=1:10
        strrep(labels{i}, '_','');
        text(0.9*max(x), max(y)-(ind/30)*max(y), labels(i),'Interpreter', 'none', 'color', [0 1 0],'VerticalAlignment','bottom', ...
                                 'HorizontalAlignment','left');
        ind=ind+1;
    end
    for i=floor(length(x)/2-4):floor(length(x)/2+5)
        strrep(labels{i}, '_','');
        text(0.9*max(x), max(y)-(ind/30)*max(y), labels(i),'Interpreter', 'none', 'color', [0 0.75 1],'VerticalAlignment','bottom', ...
                                 'HorizontalAlignment','left');
        ind=ind+1;
    end
    for i=length(x)-9:length(x)
        strrep(labels{i}, '_','');
        text(0.9*max(x), max(y)-(ind/30)*max(y), labels(i),'Interpreter', 'none','color', [1 0 0],'VerticalAlignment','bottom', ...
                                 'HorizontalAlignment','left');
        ind=ind+1;
    end
    legend('off');
    names = strsplit(fileNames, '_');
    names2 = strsplit(char(names(3)), '.');
    titleName = [char(names(1)), ' ', char(names(2)), ' ',char(names2(1))];
    title(titleName);
    xlabel(names(2));
    ylabel(names2(1));
    %print(gcf,[path,'Plots/',titleName], '-depsc');
    print(gcf,[path,'Plots/',titleName], '-dpdf');
    %=====================================================================
    fig = figure;
    hScatter = gscatter(x, y, group);
    set(gca, 'XScale', 'log');
    set(hScatter(1),'Color',[0.2 0.2 0.2]);
    set(hScatter(2),'Color',[0 1 0], 'MarkerSize', 8);%bottom values => GREEN
    set(hScatter(3),'Color',[0 0.75 1], 'MarkerSize', 8);%middle values => BLUE
    set(hScatter(4),'Color',[1 0 0], 'MarkerSize', 8);%top values => RED
    dx = 0.001; dy = 0.001; % displacement so the text does not overlay the data points
    c = [];ind =0;
    for i=1:10
        strrep(labels{i}, '_','');
        text(0.9*max(x), max(y)-(ind/30)*max(y), labels(i), 'Interpreter', 'none','color', [0 1 0],'VerticalAlignment','bottom', ...
                                 'HorizontalAlignment','left');
        ind=ind+1;
    end
    for i=floor(length(x)/2-4):floor(length(x)/2+5)
        strrep(labels{i}, '_','');
        text(0.9*max(x), max(y)-(ind/30)*max(y), labels(i),'Interpreter', 'none', 'color', [0 0.75 1],'VerticalAlignment','bottom', ...
                                 'HorizontalAlignment','left');
        ind=ind+1;
    end
    for i=length(x)-9:length(x)
        strrep(labels{i}, '_','');
        text(0.9*max(x), max(y)-(ind/30)*max(y), labels(i),'Interpreter', 'none', 'color', [1 0 0],'VerticalAlignment','bottom', ...
                                 'HorizontalAlignment','left');
        ind=ind+1;
    end
    legend('off');
    names = strsplit(fileNames, '_');
    names2 = strsplit(char(names(3)), '.');
    titleName = ['logPlot-', char(names(1)), ' ', char(names(2)), ' ',char(names2(1))];
    tName = [char(names(1)), ' ', char(names(2)), ' ',char(names2(1)), '_log_Plot'];
    title(titleName);
    xlabel(names(2));
    ylabel(['log-',char(names2(1))]);
    %print(gcf,[path,'Plots/',tName], '-depsc');
    print(gcf,[path,'Plots/',tName], '-dpdf');
    
    fig = figure;
    hScatter = gscatter(x, y, group);
    set(gca,'XScale','log');
    set(gca,'YScale','log');
    set(hScatter(1),'Color',[0.2 0.2 0.2]);
    set(hScatter(2),'Color',[0 1 0], 'MarkerSize', 8);%bottom values => GREEN
    set(hScatter(3),'Color',[0 0.75 1], 'MarkerSize', 8);%middle values => BLUE
    set(hScatter(4),'Color',[1 0 0], 'MarkerSize', 8);%top values => RED
    dx = 0.001; dy = 0.001; % displacement so the text does not overlay the data points
    c = [];ind =0; minVal = min(log10(y));
    if(isinf(min(log10(y))))
        if(max(log10(y)) == 0)
            minVal = 10^(min(log10(y(find(y)))));
            maxVal = 10^max(log10(y));
        else
            maxVal = 10^(min(log10(y(find(y)))));
            minVal = 10^max(log10(y));
        end
    end
     dVal = -(log10(maxVal) - log10(minVal));
    for i=1:10
        strrep(labels{i}, '_','');
        text(0.9*max(x), 10^(-ind*abs(dVal/30)), labels(i), 'Interpreter', 'none','color', [0 1 0],'VerticalAlignment','bottom', ...
                                 'HorizontalAlignment','left');
        ind=ind+1;
    end
    for i=floor(length(x)/2-4):floor(length(x)/2+5)
        strrep(labels{i}, '_','');
        text(0.9*max(x), 10^(-ind*abs(dVal/30)), labels(i),'Interpreter', 'none', 'color', [0 0.75 1],'VerticalAlignment','bottom', ...
                                 'HorizontalAlignment','left');
        ind=ind+1;
    end
    for i=length(x)-9:length(x)
        strrep(labels{i}, '_','');
        text(0.9*max(x), 10^(-ind*abs(dVal/30)), labels(i),'Interpreter', 'none', 'color', [1 0 0],'VerticalAlignment','bottom', ...
                                 'HorizontalAlignment','left');
        ind=ind+1;
    end
    legend('off');
    names = strsplit(fileNames, '_');
    names2 = strsplit(char(names(3)), '.');
    titleName = ['loglogPlot-', char(names(1)), ' ', char(names(2)), ' ',char(names2(1))];
    tName = [char(names(1)), ' ', char(names(2)), ' ',char(names2(1)), '_log_log_Plot'];
    title(titleName);
    xlabel(['log-',names(2)]);
    ylabel(['log-',char(names2(1))]);
    %print(gcf,[path,'Plots/',tName], '-depsc');
    print(gcf,[path,'Plots/',tName], '-dpdf');
end
%text(x, y, labels, 'color', c);