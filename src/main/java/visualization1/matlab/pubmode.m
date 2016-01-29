function pubmode(action)

%PUBMODE Set graphics properties for publication-quality graphics.
%This function was written to ensure small files (with reasonably
%good resolution) are generated from the print command. A larger
%font is used for axes labeling and a heavier line is used in
%plots. The default renderer is switched to zbuffer to ensure
%small well-resolved surface plots are created when printing
%to a file. If a graphic still requires a large amount of memory
%consider using the -r option in the print command to reduce the
%resolution. Using shading interp will allow a smaller file size than
%the default shading option for plots created using surf.
%

%zbuffer will not print hidden lines in the file so for this reason
%it creates smaller files than when the painters algorithm is used.
%When viewing the images in a postscript viewer the quality might not seem so
%good. This is because screens usually have lower resolution than printers.
%The printed image should look good in most cases.
%Daniel P. Dougherty (dpdoughe@stat.ncsu.edu)

switch action

case 'on'
h = findobj('Type','figure'); %Get handles to all figures and
%set their properties.
set(h,'PaperPosition',[2.25 4 4 3]); %Size of image in the publication
%centered on a 8.5 X 11 inch sheet
%of paper.

set(h,'Renderer','zbuffer');	%Set printing rendered to zbuffer.

h = findobj(h,'Type','axes'); %Get handles to all axes and
%set their properties.
set(h,'FontName','Times'); %Desired font.
set(h,'LineWidth',1); %Desired line width for line-plots etc.
set(h,'FontSize',16); %Fontsize of axis labels, xlabel, ylabel etc.
set(h,'FontWeight','bold'); %Font-weight

%Loop through in case there are sub-plots.
for i = 1:length(h)
set(get(h(i),'YLabel'),'FontSize',25);
set(get(h(i),'YLabel'),'FontWeight','bold');
set(get(h(i),'XLabel'),'FontSize',25);
set(get(h(i),'XLabel'),'FontWeight','bold');
set(get(h(i),'Title'),'FontSize',25);
set(get(h(i),'Title'),'FontWeight','bold');
ht = findobj(h(i),'Type','text');
set(ht,'FontSize',16);
set(ht,'FontWeight','bold');
end


%Set default properties for figures created hereafter.
set(0,'DefaultfigurePaperPosition',[2.25 4 4 3]);

set(0,'DefaultfigureRenderer','zbuffer');
set(0,'DefaultaxesFontName','Times');
set(0,'DefaultaxesLineWidth',1);
set(0,'DefaultaxesFontSize',14);
set(0,'DefaultaxesFontWeight','normal');
set(0,'DefaultaxesPosition',[0.13, 0.15 0.7132 0.75000]);

%Note that the axes property's for fonts control
%the properties for the xlabel,ylabel and title.

%Normally textual annotation for plots should be larger
%than the axis labels.
set(0,'DefaulttextFontSize',14); %Set a large font for textual
%annotation.
set(0,'DefaulttextFontWeight','bold');


case 'off'

%First un-set user-defined defaults and then undo any
%changes in existing figures and axes.

set(0,'DefaultfigurePaperPosition','remove');
set(0,'DefaultfigureRenderer','remove');
set(0,'DefaultaxesFontName','remove');
set(0,'DefaultaxesLineWidth','remove');
set(0,'DefaultaxesFontSize','remove');
set(0,'DefaultaxesFontWeight','remove');
set(0,'DefaultaxesPosition','remove');
set(0,'DefaulttextFontSize','remove');
set(0,'DefaulttextFontWeight','remove');

h = findobj('Type','figure'); %Get handles to all figures and
%set their properties.
set(h,'PaperPosition','default');
set(h,'Renderer','default');

h = findobj(h,'Type','axes'); %Get handles to all axes children and
%set their properties.
set(h,'FontName','default');
set(h,'LineWidth','default');
set(h,'FontSize','default');
set(h,'FontWeight','default');

%Need to loop through in case of subplots.
for i = 1:length(h)
set(get(h(i),'YLabel'),'FontSize','default');
set(get(h(i),'YLabel'),'FontWeight','default');
set(get(h(i),'XLabel'),'FontSize','default');
set(get(h(i),'XLabel'),'FontWeight','default');
set(get(h(i),'Title'),'FontSize','default');
set(get(h(i),'Title'),'FontWeight','default');

end

h = findobj(h,'Type','text'); %Axis labels, titles and other text...

set(h,'FontName','default');
set(h,'FontSize','default');
set(h,'FontWeight','default');


end