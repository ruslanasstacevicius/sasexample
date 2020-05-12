%inc "/department/gld/example/analysis/meta.sas";

options nomprint nomlogic nonotes nosource;

/*%let segments=ps;*/
/*%let nsegments=1;*/
/*%let lastweek=202048;*/

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


%macro get_item_W;
	%let _timer_start = %sysfunc(datetime());
	%put use_prodgrouplist=&use_prodgrouplist.;
	%if %upcase(&use_prodgrouplist.)=Y %then %do;
		data prodgroups;
			infile "&prodgrouplist_filepath." delimiter=',' MISSOVER DSD lrecl=32767 firstobs=2 obs=max;
			format prcat $&LENprcat..;
			input prcat $;
			prcat=translate(right(compress(prcat,'0D0A'x)),"0"," ");
		run;

		data _filter_prodlist_(rename=(prdid=start));
			set ref.product(%keepvars(prdid prcat)) end=last;
			if _n_=1 then do;
				declare hash sub(dataset:"prodgroups");
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

	%do w=1 %to &noweeks.;

		data item_W_&&week&w.;
			format card $1.;
			set	trans.itm_&&week&w.(%keepvars(bskid prdid strid hhdid val qty));
			by bskid prdid;

			%if %upcase(&use_prodgrouplist.)=Y %then %do;
				where put(prdid,$filfmt.)="Y";
			%end;

			card=ifc(missing(hhdid),"N","Y");

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

		run;

	%end;
	%put get_item_W duration: %sysfunc(putn(%sysevalf(%sysfunc(datetime()) - &_timer_start),time13.2));
%mend get_item_W;


%macro sumup(wkgr,per);

	%do a=1 %to &naggregations.;

		%let _timer_start = %sysfunc(datetime());

		data _null_;
			set aggregations(firstobs=&a. obs=&a.);
			call symput ('cl',strip(class));
			call symput ('tp',strip(put(_type_,8.)));
		run;
		%put &per.: it&a. tp=&tp. cl=&cl.;

		proc summary data=item_&wkgr._&per. nway missing;
			by bskid;
			class &cl.;
			id card hhdid;
			var val qty;
			output out=basket_it&a.(drop=_:) sum=;
		run;

		proc summary data=basket_it&a. nway missing;
		  class card hhdid  &cl.;
		  var val qty;
		  output out=hhd_it&a.(drop=_type_ rename=(_freq_=vis)) sum=;
		run;

		%if &nsegments.>0 %then %do;
			data hhd_it&a.;
				merge hhd_it&a.(in=ina) customer_segments(keep=hhdid %do s=1 %to &nsegments.; %scan(&segments.,&s.) %end;);
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
			proc summary data=hhd_it&a.(where=(card="Y")) nway missing;
				class &&cust_seg&s. &cl.;
				var vis val qty;
				output out=sumseg_it&a._seg&s.(drop=_type_ rename=(_freq_=hhd)) sum=;
			run;
		%end;

		proc summary data=hhd_it&a.(where=(card="N")) nway;
			class &cl.;
			var vis val qty;
			output out=sumnoncard_it&a.(drop=_:) sum=noncard_vis noncard_val noncard_qty ;
		run;

		data sumseg_it&a._seg0;
			merge sumseg_it&a._seg0 sumnoncard_it&a.;
			by &cl.;
		run;

		data allsegs_it&a.;
			format droptype 8. segnum 3. seg $3.;
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
					drop &&cust_seg&s.;
				end;
			%end;
		run;

		data allsegs_it&a.;
			merge allsegs_it&a.(in=ina) allsegs_it1(keep=segnum seg hhd rename=(hhd=tothhd));
			by segnum seg;
			if ina;
		run;

		%put Sum duration &wkgr. &per. it&a. cl &cl. : %sysfunc(putn(%sysevalf(%sysfunc(datetime()) - &_timer_start),time13.2));

	%end;

	data periodsum_&wkgr._&per.;
		format weekgroup $1. period $20.;
		attrib droptype &groups. segnum seg vis val qty hhd tothhd label="";
		set
		%do a=1 %to &naggregations.;
			allsegs_it&a.
		%end;
		;
		retain weekgroup "&wkgr.";
		retain period "&per.";
	run;

/*	proc datasets nolist nowarn nodetails;*/
/*		delete	*/
/*		%do a=1 %to &naggregations.;*/
/*			basket_it&a. hhd_it&a. hhdseg_it&a. allsegs_it&a.*/
/*			%do s=0 %to &nsegments.; sumseg_it&a._seg&s. %end;*/
/*		%end;*/
/*		;*/
/*	run;*/

	proc sort data=periodsum_&wkgr._&per.;
		by droptype &groups. segnum seg;
	run;

%mend sumup;

/*%let lastweek=202048;*/
/*%let use_prodgrouplist=N;*/
/*%let manuf=EXAMPLE;*/
/*%let groups=prdep;*/
/*%let hierarchies=;*/
/*%let segments=ps;*/
/*%let weekgroups=W;*/

%macro tracker(lastweek,use_prodgrouplist,manuf,groups,hierarchies,segments,weekgroups=W);

	data aggregations(drop=i j n h hlist hstr lastweek_startdate);
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
		lastweek_startdate=input(substr("&lastweek.",1,4)||"W"||substr("&lastweek.",5,2)||"01", weekv9.);
		call symput('midweek'  , compress(put(intnx("weekv", lastweek_startdate,  -52+1),yyweekv7.),"W"));
		call symput('firstweek', compress(put(intnx("weekv", lastweek_startdate, -104+1),yyweekv7.),"W"));
	run;

	%let nsegments=%eval(%sysfunc(countw(dummy &segments.))-1);
	%let nweekgroups=%sysfunc(countw(&weekgroups.));
	%put ngroups=&ngroups.;
	%put naggregations=&naggregations.;
	%put nsegments=&nsegments.;
	%put firstweek=&firstweek.;
	%put midweek=&midweek.;
	%put lastweek=&lastweek.;
	%put weekgroups=&weekgroups.;

	%let prodgrouplist_filepath=&deploy./analysis/reports/csv/lists/prodgrouplist_&manuf..csv;
	%let noweeks=%week_array(&firstweek.,&lastweek.);

	data periods;
		format weekgroup $1. period $20. pnum a b 3. week_a week_b $6.;
		do wgr=1 to countw("&weekgroups.");
			weekgroup=scan("&weekgroups.",wgr);
			weeks_in_period=1;
			if weekgroup="B" then weeks_in_period=2;
			if weekgroup="F" then weeks_in_period=4;
			if weekgroup="Q" then weeks_in_period=13;
			count=0;
			a=1;
			b=weeks_in_period;
			do while(b<=&noweeks.);
				week_a=symget('week'||strip(put(a,3.)));
				week_b=symget('week'||strip(put(b,3.)));
				period=ifc(week_a=week_b,week_a,week_a||"_"||week_b);
				pnum=mod(count,int(52/weeks_in_period))+1;
				count+1;
				output;
				a=a+weeks_in_period;
				b=b+weeks_in_period;
			end;
		end;
	run;

	proc sort data=periods;
		by weekgroup period;
	run;

	%create_customer_segments;
	%get_item_W;

	%do iwg=1 %to %sysfunc(countw(&weekgroups.));

		%let weekgr=%scan(&weekgroups.,&iwg.);
		%put weekgr=&weekgr.;

		%if &weekgr.=W %then %do;
			%do w=1 %to &noweeks.;
				%put Week summing: &&week&w.;  
				%sumup(&weekgr.,&&week&w.);
			%end;
			data part_&weekgr.;
				set %do w=1 %to &noweeks.; periodsum_&weekgr._&&week&w. %end;;
				by weekgroup period;
			run;
		%end;
		%else %do;
			data _null_;
				set periods(where=(weekgroup="&weekgr.")) end=last;
				call symput('a'||strip(put(_n_,3.)),strip(put(a,3.)));
				call symput('b'||strip(put(_n_,3.)),strip(put(b,3.)));
				call symput('period'||strip(put(_n_,3.)),strip(period));
				if last then call symput('noperiods',strip(put(_n_,3.)));
			run;
			%put noperiods=&noperiods.;
			%do p=1 %to &noperiods.;
				%put === Collect for period: &weekgr. a=&&a&p. b=&&b&p. period=&&period&p.;
				%let _timer_start = %sysfunc(datetime());
				data item_&weekgr._&&period&p.; /*/ view=item_&weekgr._&&period&p.;*/
					set
					%do i=&&a&p. %to &&b&p.;
						item_w_&&week&i.
					%end;
					;
					by bskid prdid;
				run;
				%put Collect for period duration &weekgr. a=&&a&p. b=&&b&p. period=&&period&p.: %sysfunc(putn(%sysevalf(%sysfunc(datetime()) - &_timer_start),time13.2));
				%sumup(&weekgr.,&&period&p.);
				proc datasets nolist nowarn nodetails;
					delete item_&weekgr._&&period&p.;
				run;
			%end;
			data part_&weekgr.;
				set %do p=1 %to &noperiods.; periodsum_&weekgr._&&period&p. %end;;
				by weekgroup period;
			run;
		%end;

	%end;

/*	proc datasets nolist nowarn nodetails;*/
/*		delete item_W_:;*/
/*	run;*/

	data result_weekgroups;
		set %do iwg=1 %to %sysfunc(countw(&weekgroups.)); part_%scan(&weekgroups.,&iwg.) %end;;
		by weekgroup period;
	run;

	proc datasets nolist nowarn nodetails;
		delete periodsum_: part_:;
	run;

	%descriptions;

	data all;

		format weekgroup $1. pnum 3. period $20. droptype 8. chartype $20. %do i=1 %to &ngroups.; %scan(&groups.,&i.) $100. %end;;
		%do i=%eval(&ngroups.+1) %to 3; dropdown&i.="ALL"; %end;
		format segnum 3. seg $100.;

		merge periods(keep=weekgroup period pnum) result_weekgroups;
		by weekgroup period;

		if segnum=0 then seg="ALL";
		chartype=substr(put(droptype,binary8.),8-max(&ngroups.,1)+1,max(&ngroups.,1));

		if _n_=1 then do;
			%do i=1 %to &ngroups.;
				%let g=%scan(&groups.,&i.);
				format &g.ds $100.;
				declare hash h&g.(dataset: "&g.ds");
				h&g..definekey("&g.");
				h&g..definedata("&g.ds");
				h&g..definedone();
			%end;
/*			%do i=1 %to &nsegments.;*/
/*				%let cust_seg&i.=%scan(&segments.,&i.);*/
/*				format &&cust_seg&i..ds $100.;*/
/*				declare hash h&&cust_seg&i.(dataset: "&&cust_seg&i..ds(rename=(&&cust_seg&i.=seg))");*/
/*				h&&cust_seg&i...definekey("seg");*/
/*				h&&cust_seg&i...definedata("&&cust_seg&i..ds");*/
/*				h&&cust_seg&i...definedone();*/
/*			%end;*/
		end;

		%do i=1 %to &ngroups.;
			%let g=%scan(&groups.,&i.);
			rcg=h&g..find()=0;
			if substr(chartype,&i.,1)="0" then &g="ALL";
			else &g.=strip(&g.)||ifc(missing(&g.ds),""," - ")||strip(&g.ds);
			drop &g.ds rcg;
			rename &g.=dropdown&i.;
		%end;

/*		%do i=1 %to &nsegments.;*/
/*			%let cust_seg&i.=%scan(&segments.,&i.);*/
/*			if segnum=&i. then do;*/
/*				rcs=h&&cust_seg&i...find()=0;*/
/*				seg="&prefix."||strip(seg)||"-"||&&cust_seg&i..ds;*/
/*			end;*/
/*			drop &&cust_seg&i..ds rcs;*/
/*		%end;*/

	run;

	data all_p0;
		merge all(rename=(period=yag_period) where=(yag_period<"&midweek.")) periods(keep=weekgroup period pnum where=(period>="&midweek."));
		by weekgroup pnum;
		rename val=yag_val qty=yag_qty vis=yag_vis hhd=yag_hhd tothhd=yag_tothhd noncard_val=yag_noncard_val noncard_qty=yag_noncard_qty noncard_vis=yag_noncard_vis;
	run;

	data all_p1;
		merge all(where=(period>="&midweek.")) periods(keep=weekgroup period pnum rename=(period=yag_period) where=(yag_period<"&midweek."));
		by weekgroup pnum;
	run;

	data data1_to_xls(drop=pnum);
		merge all_p1 all_p0;
		by weekgroup pnum period yag_period droptype dropdown: segnum seg;
		array z _numeric_;
		do over z; if z=. then z=0; end;
		if _n_=1 then do;
			lastweek="&lastweek.";
			%do i=1 %to &ngroups.;
				name&i.="%scan(&groups.,&i.)";
			%end;
			%do i=%eval(&ngroups.+1) %to 3; name&i.="none"; %end;
		end;
	run;

	%let exdir=&deploy./analysis/reports/tracker/csv/%sysfunc(compress(&manuf.,,D));

	%let gnames=;
	%let sep=;
	%do i=1 %to &ngroups.;
		%let gnames=&gnames.&sep.%scan(&groups.,&i.);
		%let sep=_;
	%end;

	proc export dbms=xlsx replace data=data1_to_xls outfile="&exdir./TrFull_&manuf.-&gnames.-&lastweek..xlsx";
	run;

%mend tracker;

*** VARIOUS TESTING. Uncomment meta include at the top;


%let _global_timer_start = %sysfunc(datetime());

%tracker(
lastweek=202048,
use_prodgrouplist=N,
manuf=EXAMPLE,
groups=prdep prcat prbrn,
hierarchies=prdep_prcat_prbrn,
segments=ps rfv,
weekgroups=W B F Q);

%put FULL DURATION: %sysfunc(putn(%sysevalf(%sysfunc(datetime()) - &_global_timer_start),time13.2));
