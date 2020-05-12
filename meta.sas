
%let deploy=/department/gld/example;
libname trans "&deploy./pdl/trans" access=readonly;
libname ref "&deploy./pdl/ref" access=readonly;
libname seg "&deploy./pdl/seg" access=readonly;

data _meta_;
infile datalines dsd;
format fullname $32. shortname $8. variable_length 8.;
input fullname $ shortname $ variable_length;
if missing(variable_length) then variable_length=8;
call symput ('n'||compress(shortname,,"s"),compress(fullname,,"s"));
call symput ('L'||compress(fullname,,"s"),compress(put(variable_length,3.),,"s"));
call symput ('LEN'||compress(shortname,,"s"),compress(put(variable_length,3.),,"s"));
datalines;
basket_id,	bskid,	8
household_id,	hhdid,	6
week_id,	week,	6
tr_date,	date,	
tr_time,	time,	
store_id,	strid,	2
store_desc,	stridds,100
region_id,	streg,	1
region_desc,	stregds,100
format_id,	strfm,	1
format_desc,	strfmds,100
sales_value,	val,,
sales_quantity,	qty,,
sales_value,	itmvl,,
sales_quantity,	itmqt,,
product_id,	prdid,	5
department_id,	prdep,	2
category_id,	prcat,	3
brand_id,	prbrn,	3
product_desc,prdidds,100
department_desc,prdepds,100
category_desc,	prcatds,100
brand_desc,	prbrnds,100
;
run;

%macro keepvars(varlist);
	%let nvar=%sysfunc(countw(&varlist.));
	%let keepstring=;
	%let renstring=;
	%local __meta_i__ __meta_var__;
	%do __meta_i__=1 %to &nvar.;
		%let __meta_var__=%scan(&varlist.,&__meta_i__.);
		%let keepstring=&keepstring. &&n&__meta_var__.;
		%let renstring=&renstring. &&n&__meta_var__.=&__meta_var__.;
	%end;
keep=&keepstring. rename=(&renstring.)
%mend;

*** WEEK UTILITY MACROS;

*** shift days if week starts not on monday;
%let nwkday=1;
*  1 - Monday;
*  2 - Tuesday;
*  3 - Wednesday;
*  4 - Thursday;

%macro week_start_date(wkstr);
    %local yr wk startdt;
    %let yr=%substr(%sysfunc(compress(&wkstr.,%str(%'%"))),1,4);
    %let wk=%substr(%sysfunc(compress(&wkstr.,%str(%'%"))),5,2);
    %let startdt=%eval(%sysfunc(intnx(week.2,%sysevalf("04JAN&yr."D),0)) + 7*(&wk.-1) - &nwkday.);
    "%sysfunc(putn(&startdt.,date9.))"D
%mend week_start_date;


%macro week_end_date(wkstr);
    %local yr wk startdt;
    %let yr=%substr(%sysfunc(compress(&wkstr.,%str(%'%"))),1,4);
    %let wk=%substr(%sysfunc(compress(&wkstr.,%str(%'%"))),5,2);
    %let startdt=%eval(%sysfunc(intnx(week.2,%sysevalf("04JAN&yr."D),0)) + 7*(&wk.-1) - &nwkday. + 6);
    "%sysfunc(putn(&startdt.,date9.))"D
%mend week_end_date;

%macro date_to_week(dt);
    %substr(%sysfunc(compress(%sysfunc(putn(%sysevalf(&dt.+&nwkday.),weekv.))," -Ww")),1,6)
%mend date_to_week;


** week in form yyyyww, e.g. 201531 - 31st week of year 2015;
%macro week_array(firstwk,lastwk);
%put META - Creating week array firstwk=&firstwk. lastwk=&lastwk.;
%local firstdt lastdt a nwk;
%let firstdt=%week_start_date(&firstwk.);
%let lastdt=%week_start_date(&lastwk.);
%let nwk=%sysevalf((&lastdt.-&firstdt.)/7+1);
%do a=1 %to &nwk.;
    %global week&a.;
    %let week&a.=%date_to_week(&firstdt.+7*(&a.-1));
/*    %put week&a.=&&week&a.;*/
%end;
&nwk.
%mend week_array;

*** DESCRIPTIONS;

%macro meta_desc(metadset,metavar);
	proc sort data=ref.&metadset(%keepvars(&metavar. &metavar.ds)) out=&metavar.ds nodupkey;
		by &metavar.;
	run;
%mend meta_desc;

%macro descriptions;
	%meta_desc(product,prdep);
	%meta_desc(product,prcat);
	%meta_desc(product,prbrn);
	%meta_desc(product,prdid);
	%meta_desc(store,strid);
	%meta_desc(store,streg);
	%meta_desc(store,strfm);
%mend descriptions;

%macro meta_format(metadset,metavar);
	proc sort data=ref.&metadset(%keepvars(&metavar. &metavar.ds)) out=_&metavar.fm_ nodupkey;
		by &metavar.;
	run;
	data _&metavar.fm_;
		set _&metavar.fm_(rename=(&metavar.=start &metavar.ds=label)) end=last; 
		retain fmtname "$&metavar.ds";
		output;
		if last then do;
			start="";
			label="N";
			hlo="o";
			output;
		end;
	run;
	proc format cntlin=_&metavar.fm_; run;
	proc delete data=_&metavar.fm_; run;
%mend meta_format;

%macro description_formats;
	%meta_format(product,prdep);
	%meta_format(product,prcat);
	%meta_format(product,prbrn);
	%meta_format(product,prdid);
	%meta_format(store,strid);
	%meta_format(store,streg);
	%meta_format(store,strfm);
%mend description_formats;


