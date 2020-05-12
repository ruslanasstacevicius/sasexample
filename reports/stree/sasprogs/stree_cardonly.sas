%inc "/department/gld/example/analysis/meta.sas";

%let now=%sysfunc(compress(%sysfunc(date(),yymmdd.),-))_%sysfunc(compress(%sysfunc(time(),tod8.),:));
%put now = &now.; 

libname stree "&deploy./analysis/reports/stree/sasdata";

/*options nomprint nomlogic nonotes nosource;*/
options   mprint   mlogic   notes   source;

%macro create_customer_segments;

%do i=1 %to &nsegments.;

	%let seg=%scan(&segments.,&i.);
	%put seg=&seg.;

	data segdsets;
		set sashelp.vmember(where=(upcase(libname)="SEG" and index(upcase(memname),upcase("&seg._20"))=1)) end=last;
		length segwk wk $6.;
		retain segwk;
		wk=substr(trim(memname),length(trim(memname))-5,6);
		segwk=ifc(wk>segwk and wk<="&lastweek.",wk,segwk);
		if last then call symput("&seg.wk",trim(segwk));
	run;

	%put &seg.wk=&&&seg.wk.;

	data &seg.;
		set seg.&seg._&&&seg.wk.(keep=hhdid &seg._seg rename=(&seg._seg=&seg.));
	run;
%end;

%if &nsegments.>0 %then %do;
	data customer_segments;
			merge %do i=1 %to &nsegments.; %scan(&segments.,&i.) %end;;
			by hhdid;
	run;
	proc datasets nolist nowarn nodetails; delete &segments.; run;
%end;

%mend create_customer_segments;


%macro get_item;

	%put use_catlist=&use_catlist.;
	%if %upcase(&use_catlist.)=Y %then %do;
		data subcats;
			infile "&catlist_filepath." delimiter=',' MISSOVER DSD lrecl=32767 firstobs=2 obs=max;
			format prcat $&LENprcat..;
			input prcat $;
			prcat=translate(right(compress(prcat,'0D0A'x)),"0"," ");
		run;

		data _filter_prodlist_(rename=(prdid=start));
			set ref.product(%keepvars(prdid prcat)) end=last;
			if _n_=1 then do;
				declare hash sub(dataset:"subcats");
				sub.definekey("prcat");
				sub.definedone();
			end;
			retain label "Y";
			retain fmtname "$filfmt";
			if sub.find()=0 then output;
			if last then do;
				prdid=" ";
				prcat=" ";
				hlo="o";
				label="N";
				output;
			end;
		run;

		proc format cntlin=_filter_prodlist_;
		run;
	%end;

	data _null_;
		format gr prodgroups storegroups $32767.;
		gr=tranwrd(tranwrd("&groups.","prdid",""),"strid","");
		prodgroups=compbl(tranwrd(tranwrd(gr,"strfm",""),"streg",""));
		storegroups=ifc(find(gr,"strfm"),"strfm","")||" "||ifc(find(gr,"streg"),"streg","");
		call symput('prodgroups',strip(prodgroups));
		call symput('storegroups',strip(storegroups));
		call symput('hashprod','"'||tranwrd(strip(prodgroups)," ",'","')||'"');
		call symput('hashstore','"'||tranwrd(strip(storegroups)," ",'","')||'"');
	run;

	%put prodgroups=&prodgroups.;
	%put hashprod=&hashprod.;
	%put storegroups=&storegroups.;
	%put hashstore=&hashstore.;

	data
	%do p=1 %to %sysfunc(countw(&pdefinitions.));
		%let pdef=%scan(&pdefinitions.,&p.);
		item_&pdef._0 item_&pdef._1
	%end;
	;
		set
			%do w=1 %to &nwk.;
				trans.itm_&&week&w.(in=inwk&w. %keepvars(bskid date prdid strid val qty hhdid))
			%end;
		;
		by bskid prdid;

		format pdef $3. p0_start p0_end p1_start p1_end yymmdd10.;
		if _n_=1 then do;
			declare hash hp(dataset:"periods");
			hp.definekey("pdef");
			hp.definedata("p0_start", "p0_end", "p1_start", "p1_end");
			hp.definedone();
		end;

		where hhdid not in ("","0") %if %upcase(&use_catlist.)=Y %then %do; and put(prdid,$filfmt.)="Y" %end;;

		%if &prodgroups.^=%str() %then %do;
			if _n_=1 then do;
				if 0 then set ref.product(%keepvars(&prodgroups.));
				dcl hash hprod(dataset:"ref.product(%keepvars(prdid &prodgroups.))");
				hprod.definekey("prdid");
				hprod.definedata(&hashprod.);
				hprod.definedone();
			end;
			call missing(%sysfunc(compress(%quote(&hashprod.),%str(%"))));
			xprod=(hprod.find()=0);
			drop xprod;
		%end;

		%if &storegroups.^=%str() %then %do;
			if _n_=1 then do;
				if 0 then set ref.store(%keepvars(&storegroups.));
				dcl hash hstore(dataset:"ref.store(%keepvars(strid &storegroups.))");
				hstore.definekey("strid");
				hstore.definedata(&hashstore.);
				hstore.definedone();
			end;
			call missing(%sysfunc(compress(%quote(&hashstore.),%str(%"))));
			xstore=(hstore.find()=0);
			drop xstore;
		%end;

		%do i=1 %to &ngroups.;
			%let g=%scan(&groups.,&i.);
			if missing(&g.) then &g.="x";
		%end;

		%do p=1 %to %sysfunc(countw(&pdefinitions.));
			%let pdef=%scan(&pdefinitions.,&p.);
			pdef="&pdef";
			if hp.find()=0 and date>=p0_start and date<=p0_end then output item_&pdef._0;
			if hp.find()=0 and date>=p1_start and date<=p1_end then output item_&pdef._1;
		%end;

		drop date pdef p0_: p1_:;

	run;

%mend get_item;


%macro L4L_format;
	%do p=1 %to %sysfunc(countw(&pdefinitions.));
		%let pdef=%scan(&pdefinitions.,&p.);
		%do i=0 %to 1;
			proc summary data=item_&pdef._&i. nway missing;
				class strid;
				var val;
				output out=storelist_&pdef._&i.(drop=_:) sum=;
			run;
		%end;
		data L4L_format_&pdef.(rename=(strid=start));
			merge storelist_&pdef._0(in=in0) storelist_&pdef._1(in=in1) end=last;
			by strid;
			retain fmtname "$L4L&pdef.";
			retain label "Y";
			if in0 and in1 then output;
			if last then do;
				strid="";
				val=0;
				hlo="o";
				label="N";
				output;
			end;
		run;
		proc format cntlin=L4L_format_&pdef.;
		run;
	%end;
	proc datasets nolist nowarn nodetails;
		delete storelist_: L4L_format_:;
	run;
%mend L4L_format;


%macro sumup(perdef,per);

	%do L4L=0 %to 1;

		%do a=1 %to &naggregations.;

			data _null_;
				set aggregations(firstobs=&a. obs=&a.);
				call symput ('cl',strip(class));
				call symput ('tp',strip(put(_type_,8.)));
			run;

			%let _timer_ = %sysfunc(datetime());

			proc summary data=item_&perdef._&per. nway missing;
				%if &L4L=1 %then where put(strid,$L4L&perdef..)="Y";;
				by bskid;
				class &cl.;
				id hhdid;
				var val qty;
				output out=basket_it&a.(drop=_:) sum=;
			run;

			proc summary data=basket_it&a. nway missing;
			  class hhdid  &cl.;
			  var val qty;
			  output out=hhdseg_it&a.(drop=_type_ rename=(_freq_=vis)) sum=;
			run;

			proc datasets nolist nowarn nodetails;
				delete basket_it&a.;
			run;

			%if &nsegments.>0 %then %do;
				data hhdseg_it&a.;
					merge hhdseg_it&a.(in=ina) customer_segments(keep=hhdid %do s=1 %to &nsegments.; %scan(&segments.,&s.) %end;);
					by hhdid;
					if ina;
					%do s=1 %to &nsegments.;
						%let cust_seg&s.=%scan(&segments.,&s.);
						if missing(&&cust_seg&s.) then &&cust_seg&s.="xx";
					%end;
				run;
			%end;

			%do s=0 %to &nsegments.;
				%if &s.=0 %then %let cust_seg0=;
				%if &s.>0 %then %let cust_seg&s.=%scan(&segments.,&s.);
				proc summary data=hhdseg_it&a. nway missing;
					class &&cust_seg&s. &cl.;
					var vis val qty;
					output out=sumseg_it&a._seg&s.(drop=_type_ rename=(_freq_=hhd)) sum=;
				run;
			%end;

			proc datasets nolist nowarn nodetails;
				delete hhdseg_it&a.;
			run;

			data sum_allsegs_it&a.;
				format droptype 8. segnum 3. seg $8.;
				set
				%do s=0 %to &nsegments.;
					%if &s.=0 %then %let cust_seg0=;
					%if &s.>0 %then %let cust_seg&s.=%scan(&segments.,&s.);
					sumseg_it&a._seg&s.(in=in&s.)
				%end;
				;
				droptype=&tp.;
				segnum=0;
				%do s=1 %to &nsegments.;
					%let cust_seg&s.=%scan(&segments.,&s.);
					if in&s. then do;
						segnum=&s.;
						seg=&&cust_seg&s.;
						if seg="xx" then seg="xx"||"_&&cust_seg&s.";
						drop &&cust_seg&s.;
					end;
				%end;
			run;

			data sum_allsegs_it&a.;
				merge sum_allsegs_it&a.(in=ina) sum_allsegs_it1(keep=segnum seg hhd rename=(hhd=tothhd));
				by segnum seg;
				if ina;
			run;

			%put &perdef.&per.: L4L=&L4L. it&a. tp=&tp. cl=&cl.   SUMTIME: %sysfunc(putn(%sysevalf(%sysfunc(datetime()) - &_timer_),time13.2));

		%end;

		data periodsum_&perdef._&per._&L4L.;
			format L4L $1. perdef $3.;
			attrib droptype &groups. segnum seg vis val qty hhd tothhd label="";
			retain L4L "&L4L.";
			retain perdef "&perdef.";
			set	%do a=1 %to &naggregations.; sum_allsegs_it&a. %end;;
		run;

		proc sort data=periodsum_&perdef._&per._&L4L.;
			by droptype &groups. segnum seg; 
		run;

		proc datasets nolist nowarn nodetails;
			delete all: count: sum: basket: hhd: list_:;
		run;

	%end; *L4L;

	data periodsum_&perdef._&per.;
		set periodsum_&perdef._&per._0 periodsum_&perdef._&per._1;
		by L4L droptype &groups. segnum seg;
	run;

	proc datasets nolist nowarn nodetails;
		delete periodsum_&perdef._&per._:;
	run;

%mend sumup;

/*%let month=2017OCT;*/
/*%let use_catlist=Y;*/
/*%let manuf=BABY;*/
/*%let groups=prsct;*/
/*%let hierarchies=;*/
/*%let segments=;*/

%macro sales_tree(month,use_catlist,manuf,groups,hierarchies,segments);

	data periods(drop=first_date last_date);
		format pdef $3.;
		format first_date last_date p0_start p0_end p1_start p1_end date9.;
		retain first_date last_date;

		last_date=intnx("month",input("01"||substr("&month.",5,3)||substr("&month.",1,4),date9.),0,"end");
		first_date=intnx("month",last_date,-23,"beginning");

		pdef="MAT";
		p0_start=intnx("month",last_date,-23,"beginning");
		p0_end=intnx("month",last_date,-12,"end");
		p1_start=intnx("month",last_date,-11,"beginning");
		p1_end=last_date;
		output;

		pdef="YTD";
		p0_start=intnx("year",last_date,-1,"beginning");
		p0_end=intnx("month",last_date,-12,"end");
		p1_start=intnx("year",last_date,0,"beginning");
		p1_end=last_date;
		output;

		pdef="MPR";
		p0_start=intnx("month",last_date,-1,"beginning");
		p0_end=intnx("month",last_date,-1,"end");
		p1_start=intnx("month",last_date,0,"beginning");
		p1_end=last_date;
		output;

		pdef="MYA";
		p0_start=intnx("month",last_date,-12,"beginning");
		p0_end=intnx("month",last_date,-12,"end");
		p1_start=intnx("month",last_date,0,"beginning");
		p1_end=last_date;
		output;

		call symput('firstweek',compress(put(intnx("weekv",first_date,0),yyweekv7.),"W"));
		call symput('lastweek',compress(put(intnx("weekv",last_date,0),yyweekv7.),"W"));

	run;

	%put firstweek=&firstweek.;
	%put lastweek=&lastweek.;

	proc sql noprint;
		select pdef into :pdefinitions separated by " " from periods;
	quit;

	%let catlist_filepath=&deploy./analysis/reports/csv/lists/catlist_&manuf..csv;

	data aggregations(drop=i j n h hlist hstr);
		format _type_ count 8. chartype class hstr hlist $32767.;
		n=countw("&groups.");
		h=countw("&hierarchies.");
		do _type_=0 to 2**n-1;
			chartype=substr(put(_type_,binary8.),8-max(n,1)+1,max(n,1));
			class="";
			del=0;
			do i=1 to n;
				if band(_type_,2**(n-i)) then class=strip(trim(class)||" "||scan("&groups.",i));			
			end;
			*hierarchy check;
			do i=1 to h;
				hlist=translate(scan("&hierarchies.",i)," ","_");
				hstr="";
				do j=1 to countw(hlist);
					hstr=strip(hstr)||substr(chartype,findw("&groups.",scan(hlist,j),"","e"),1);
				end;
				if find(hstr,"01")>0 then del=1;
			end;
			count+ifn(del=0,1,0);
			if del=0 then output;
		end;
		call symput('ngroups',strip(put(n,8.)));
		call symput('naggregations',strip(put(count,8.)));
	run;

	%let nsegments=%eval(%sysfunc(countw(dummy &segments.))-1);
	%put ngroups=&ngroups.;
	%put naggregations=&naggregations.;
	%put nsegments=&nsegments.;
	%put pdefinitions=&pdefinitions.;

	%let nwk=%week_array(&firstweek.,&lastweek.);
	%get_item;
	%L4L_format;
	%create_customer_segments;

	%do p=1 %to %sysfunc(countw(&pdefinitions.));
		%let pdef=%scan(&pdefinitions.,&p.);
		%sumup(&pdef.,0);
		proc datasets nolist nowarn nodetails;
			delete item_&pdef._0;
		run;
		%sumup(&pdef.,1);
		proc datasets nolist nowarn nodetails;
			delete item_&pdef._1;
		run;
	%end;

	%do per=0 %to 1;
		%if &per.=0 %then %let prefix=p0_;
		%if &per.=1 %then %let prefix=p1_;
		data all&per.;
			set
			%do p=1 %to %sysfunc(countw(&pdefinitions.));
				%let pdef=%scan(&pdefinitions.,&p.);
				periodsum_&pdef._&per.(rename=(vis=&prefix.vis val=&prefix.val qty=&prefix.qty hhd=&prefix.hdd tothhd=&prefix.tothhd))
			%end;
			;
			by L4L perdef droptype &groups. segnum seg;
		run;
	%end;

	data _null_;
		set periods;
		call symput(strip(pdef)||'_p0_start',strip(put(p0_start,date9.)));
		call symput(strip(pdef)||'_p0_end',strip(put(p0_end,date9.)));
		call symput(strip(pdef)||'_p1_start',strip(put(p1_start,date9.)));
		call symput(strip(pdef)||'_p1_end',strip(put(p1_end,date9.)));
	run;

	%descriptions;

	data data1_to_xls;

		format L4L $1. perdef $3. droptype 8. chartype $20. %do i=1 %to &ngroups.; %scan(&groups.,&i.) $100. %end;;
		%do i=%eval(&ngroups.+1) %to 3; dropdown&i.="ALL"; %end;
		format segnum 3. seg $100.;

		merge all1 all0;
		by L4L perdef droptype &groups. segnum seg;

		if segnum=0 then seg="ALL";
		chartype=substr(put(droptype,binary8.),8-max(&ngroups.,1)+1,max(&ngroups.,1));

		if _n_=1 then do;

			%do i=1 %to &ngroups.;
				name&i.="%scan(&groups.,&i.)";
			%end;
			%do i=%eval(&ngroups.+1) %to 3; name&i.="none"; %end;

			%do p=1 %to %sysfunc(countw(&pdefinitions.));
				%let pdef=%scan(&pdefinitions.,&p.);
				&pdef._p0_start="&&&pdef._p0_start.";
				&pdef._p0_end="&&&pdef._p0_end.";
				&pdef._p1_start="&&&pdef._p1_start.";
				&pdef._p1_end="&&&pdef._p1_end.";
			%end;

			%do i=1 %to &ngroups.;
				%let g=%scan(&groups.,&i.);
				format &g.ds $100.;
				declare hash h&g.(dataset: "&g.ds");
				h&g..definekey("&g.");
				h&g..definedata("&g.ds");
				h&g..definedone();
			%end;
		end;

		%do i=1 %to &ngroups.;
			%let g=%scan(&groups.,&i.);
			rcg=h&g..find()=0;
			if substr(chartype,&i.,1)="0" then &g="ALL";
			else &g.=strip(&g.)||ifc(missing(&g.ds),""," - ")||strip(&g.ds);
			drop &g.ds rcg;
			rename &g.=dropdown&i.;
		%end;

		array z _numeric_;
		do over z; if z=. then z=0; end;
	run;

	%let exdir=&deploy./analysis/reports/stree/csv/%sysfunc(compress(&manuf.,,D));

	%let gnames=;
	%let sep=;
	%do i=1 %to &ngroups.;
		%let gnames=&gnames.&sep.%scan(&groups.,&i.);
		%let sep=_;
	%end;

	data stree.&manuf._&month._&now.;
		set data1_to_xls end=last;
		if last then call symput("numrecords",compress(put(_n_,8.)));
	run;

	%let fileformat=xlsx;
	%if &numrecords.>1048576 %then %let fileformat=csv;	

	proc export dbms=&fileformat. replace data=data1_to_xls outfile="&exdir./STree_&manuf.-&gnames.-&lastweek._&month..&fileformat.";
	run;


%mend sales_tree;

*** VARIOUS TESTING. Uncomment meta include at the top;

%let _global_timer_start = %sysfunc(datetime());
%sales_tree(
month=2020OCT,
use_catlist=N,
manuf=EXAMPLE,
groups=prdep prcat prbrn strfm,
hierarchies=prdep_prcat_prbrn,
segments=ps);
%put FULL DURATION: %sysfunc(putn(%sysevalf(%sysfunc(datetime()) - &_global_timer_start),time13.2));
