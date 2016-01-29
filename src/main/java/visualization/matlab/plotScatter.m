path = 'Politics/';
Files=dir([path,'*.*']);
for k=1:length(Files)
   fileNames=Files(k).name;
   if (strcmp(fileNames(1),'.'))
       continue;
   end
   data1 = csvread([path,fileNames]);
   fig = figure;
    scatter(data1(:,1), data1(:,2));
    names = strsplit(fileNames, '_');
    names2 = strsplit(char(names(3)), '.');
    titleName = [char(names(1)), ' ', char(names(2)), ' ',char(names2(1))];
    title(titleName);
    xlabel(names(2));
    ylabel(names2(1));
    print(fig,[path,'Plots/',titleName], '-depsc');
    print(fig,[path,'Plots/',titleName], '-dpdf');
end