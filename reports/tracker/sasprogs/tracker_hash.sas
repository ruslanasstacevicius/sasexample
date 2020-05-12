
%inc "/department/gld/example/analysis/meta.sas";

%let _global_timer_start = %sysfunc(datetime());

options nosource nonotes nomprint nomlogic;


%macro create_customer_segments(seglastwk,seglist,numbered_names=N);

	%local numseg sg;
	%let numseg=%eval(%sysfunc(countw(dummy &seglist.))-1);
	%if &numseg.<1 %then %return;

	%do i=1 %to &numseg.;

		%let sg=%scan(&seglist.,&i.);

		data segdsets;
			set sashelp.vmember(where=(upcase(libname)="SEG" and index(upcase(memname),upcase("&sg._20"))=1)) end=last;
			length segwk wk $6.;
			retain segwk;
			wk=substr(trim(memname),length(trim(memname))-5,6);
			segwk=ifc(wk>segwk and wk<="&seglastwk.",wk,segwk);
			if last then call symput("&sg.wk",trim(segwk));
		run;

		data &sg.;
			set seg.&sg._&&&sg.wk.(keep=hhdid &sg._seg);
			%if &numbered_names.=Y %then rename &sg._seg=seg&i.;;
		run;

		%put sg=&sg. &sg.wk=&&&sg.wk.;

	%end;

	data customer_segments;
			merge &seglist.;
			by hhdid;
	run;

	proc datasets nolist nowarn nodetails;
		delete &seglist.;
	run;

%mend create_customer_segments;


%macro create_prodfilter_format(use_reallist,path,filtergroup);

		%if &use_reallist.^=Y %then %do;
			proc format;
				value $filtfmt other="Y";				
			run;
			%return;
		%end;

		data prodgroups;
			if 0 then set ref.product(%keepvars(&filtergroup.));
			infile "&path." delimiter=',' MISSOVER DSD lrecl=32767 firstobs=2 obs=max;
			input &filtergroup. $;
			&filtergroup.=translate(right(compress(&filtergroup.,'0D0A'x)),"0"," ");
		run;

		data _filter_prodlist_(rename=(prdid=start));
			set ref.product(%keepvars(prdid &filtergroup.)) end=last;
			if _n_=1 then do;
				declare hash grfilter(dataset:"prodgroups");
				grfilter.definekey("&filtergroup.");
				grfilter.definedone();
			end;
			retain label "Y";
			retain fmtname "$filtfmt";
			if grfilter.find()=0 then output;
			if last then do;
				prdid=" ";
				&filtergroup.=" ";
				hlo="o";
				label="N";
				output;
			end;
		run;

		proc format cntlin=_filter_prodlist_;
		run;

%mend create_prodfilter_format;


%macro get_weekly_transactions(firstwk, lastwk, grlist, nseg, use_filter=N, keepdt=N, custseg_dset=);
	%let _timer_start = %sysfunc(datetime());
	data _null_;
		format gr prodgroups storegroups $32767.;
		gr=tranwrd(tranwrd("&grlist.","prdid",""),"strid","");
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

	%let datevar=;
	%if &keepdt.=Y %then %let datevar=date;
	%let nweeks=%week_array(&firstwk.,&lastwk.);

	%do w=1 %to &nweeks.;

		data card_item_W_&&week&w. noncard_item_W_&&week&w.(drop=hhdid);

			set trans.itm_&&week&w.(%keepvars(bskid prdid &datevar. strid hhdid val qty) %if &use_filter.=Y %then where=(put(prdid,$filtfmt.)="Y"););
			by bskid prdid;
			%if &prodgroups.^=%str() %then %do;
				if _n_=1 then do;
					if 0 then set ref.product(%keepvars(&prodgroups.));
					dcl hash hprod(dataset:"ref.product(%keepvars(prdid &prodgroups.))");
					hprod.definekey("prdid");
					hprod.definedata(&hashprod.);
					hprod.definedone();
				end;
				call missing(%sysfunc(compress(%quote(&hashprod.),%str(%"))));
				rcp=(hprod.find()=0);
				drop rcp;
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
				rcs=(hstore.find()=0);
				drop rcs;
			%end;

			%do i=1 %to %eval(%sysfunc(countw(dummy &grlist.))-1);
				%let g=%scan(&grlist.,&i.);
				if missing(&g.) then &g.="x";
			%end;

			if hhdid in (" " "0") then output noncard_item_W_&&week&w.;
			else output card_item_W_&&week&w.;

		run;

		proc sort data=card_item_W_&&week&w.;
			by hhdid bskid prdid;
		run;

		%if &nseg.>0 %then %do;
			data card_item_W_&&week&w.(drop=segnum);
				merge card_item_W_&&week&w.(in=in_item) customer_segments;
				by hhdid;
				if in_item;
				array s[&nseg.] seg1-seg&nseg.;
				do segnum=1 to &nseg.;
					if missing(s[segnum]) then s[segnum]="xx";
				end;
			run;
		%end;

	%end;
	%put Duration get_item_W: %sysfunc(putn(%sysevalf(%sysfunc(datetime()) - &_timer_start),time13.2));
%mend get_weekly_transactions;



%macro sum_noncard(dsetin,class,dsetout);
	%local segnamelen;
	%let segnamelen=$20.;
	%if &class.=%str() %then %do;
		data &dsetout.(keep=segnum segment vis val qty);
			retain segnum 0 segment "ALL" vis val qty 0;
			set &dsetin.(rename=(val=v qty=q)) end=last;
			by bskid;
			val=sum(val,v);
			qty=sum(qty,q);
			if last.bskid then vis=sum(vis,1);
			if last then output;
		run;
	%end;
	%else %do;
		data _null_;
			call symput('hashclass','"'||tranwrd(strip("&class.")," ",'","')||'"');			
		run;
		data _null_;
			retain segnum 0 segment "ALL";
			if _n_=1 or rows=0 then do;
				declare hash kvis();
				kvis.definekey("bskid",&hashclass.);
				kvis.definedone();
				declare hash hresult(ordered:"ascending");
				hresult.definekey(&hashclass.);
				hresult.definedata(&hashclass.,"segnum","segment","vis","val","qty");
				hresult.definedone();
				if rows=0 then hresult.output(dataset:"&dsetout.");
			end;
			set &dsetin.(rename=(val=v qty=q)) end=last nobs=rows;
			by bskid;
			rc=hresult.find();
			val=sum(val,v);
			qty=sum(qty,q);
			if kvis.check()^=0 then do;
				rc=kvis.add();
				vis=sum(vis,1);
			end; 
			rc=hresult.replace();
			if last.bskid then kvis.clear();
			if last then hresult.output(dataset:"&dsetout.");
		run;
	%end;
%mend sum_noncard;


%macro sum_card(dsetin,class,nseg,dsetout);
	%local segnamelen;
	%let segnamelen=$20.;
	data _null_;
		call symput('hashclass','"'||tranwrd(strip("&class.")," ",'","')||'"');			
	run;
	%if &class.=%str() %then %do;
		data _null_;
			if _n_=1 or rows=0 then do;
				%do s=0 %to &nseg.;
					declare hash hresult&s.(ordered:"ascending");
					hresult&s..definekey("seg&s.");
					hresult&s..definedata("seg&s.","vis&s.","val&s.","qty&s.","hhd&s.");
					hresult&s..definedone();
					if rows=0 then hresult&s..output(dataset: "resultseg&s.");
				%end;				
			end;
			set &dsetin.(rename=(val=v qty=q)) end=last nobs=rows;
			by hhdid bskid;
			%do s=0 %to &nseg.; format vis&s. val&s. qty&s. hhd&s. best12.; %end;
			retain seg0 "ALL";
			%do s=0 %to &nseg.;
				rc=hresult&s..find();
				val&s.=sum(val&s.,v);
				qty&s.=sum(qty&s.,q);
			%end;
				if last.bskid then do;
					%do s=0 %to &nseg.;
						vis&s.=sum(vis&s.,1);
					%end;
				end;
				if last.hhdid then do;
					%do s=0 %to &nseg.;
						hhd&s.=sum(hhd&s.,1);
					%end;
				end;
			%do s=0 %to &nseg.;
				rc=hresult&s..replace();
			%end;

			if last then do;
				%do s=0 %to &nseg.;
					hresult&s..output(dataset: "resultseg&s.");
				%end;
			end;
		run;
	%end;
	%else %do;
		data _null_;
			if _n_=1 or rows=0 then do;
				declare hash kvis();
				kvis.definekey("bskid",&hashclass.);
				kvis.definedone();
				declare hash khhd();
				khhd.definekey("hhdid",&hashclass.);
				khhd.definedone();
				%do s=0 %to &nseg.;
					declare hash hresult&s.(ordered:"ascending");
					hresult&s..definekey(&hashclass.,"seg&s.");
					hresult&s..definedata(&hashclass.,"seg&s.","vis&s.","val&s.","qty&s.","hhd&s.");
					hresult&s..definedone();
					if rows=0 then hresult&s..output(dataset: "resultseg&s.");
				%end;	
			end;
			set &dsetin.(rename=(val=v qty=q)) end=last nobs=rows;
			by hhdid bskid;
			retain seg0 "ALL";
			%do s=0 %to &nseg.; format vis&s. val&s. qty&s. hhd&s. best12.; %end;
			%do s=0 %to &nseg.;
				rc=hresult&s..find();
				val&s.=sum(val&s.,v);
				qty&s.=sum(qty&s.,q);
			%end;
			if kvis.check()^=0 then do;
				rc=kvis.add();
				%do s=0 %to &nseg.;	vis&s.=sum(vis&s.,1); %end;
				if khhd.check()^=0 then do;
					rc=khhd.add();
					%do s=0 %to &nseg.;	hhd&s.=sum(hhd&s.,1); %end;
				end;
			end;
			%do s=0 %to &nseg.;
				rc=hresult&s..replace();
			%end;
			if last.bskid then kvis.clear();
			if last.hhdid then khhd.clear();
			if last then do;
				%do s=0 %to &nseg.;
					hresult&s..output(dataset: "resultseg&s.");
				%end;
			end;
		run;
	%end;
	data &dsetout.;
		attrib &class. segnum segment label="";
		format segnum 3. segment &segnamelen.;
		set %do s=0 %to &nseg.; resultseg&s.(in=in&s. rename=(vis&s.=vis val&s.=val qty&s.=qty hhd&s.=hhd)) %end;;
		by &class.;
		%do s=0 %to &nseg.; 
			if in&s. then do;
				segnum=&s.;
				segment=strip(seg&s.);
			end;
			drop seg&s.;
		%end;
	run;
	proc delete data=%do s=0 %to &nseg.; resultseg&s. %end;;
	run;
%mend sum_card;

%macro tracker(lastweek,use_prodgrouplist,manuf,groups,hierarchies,segments,periodgroups=W);

	%let prodgrouplist_filepath=&deploy./analysis/reports/lists/prodgrouplist_&manuf..csv;

	data aggregations(drop=i j n h hlist hstr);
		format _type_ count 8. chartype $20. class hstr hlist $32767.;
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
	%let nperiodgroups=%sysfunc(countw(&periodgroups.));
	%put ngroups=&ngroups.;
	%put naggregations=&naggregations.;
	%put nsegments=&nsegments.;

	data periods(keep=dt_: firstdt lastdt startwk: endwk: period: mon);
		format firstdt lastdt lastmonthdt dt_thisyear dt_yearago yymmdd10. startwk0 endwk0 startwk1 endwk1 $6. periodicity $1. periodname1 periodname0 $20. mon 3.;
		lastdt=input(substr("&lastweek.",1,4)||"W"||substr("&lastweek.",5,2)||"07",weekv9.);;
		firstdt=lastdt-7*104+1;
		weekgroups=upcase(compress("&periodgroups.","M"));
		keepdate="N";
		do p=1 to countw(weekgroups);
			periodicity=scan(weekgroups, p);
			step=ifn(periodicity="B",2,ifn(periodicity="F",4,ifn(periodicity="Q",13,1)));
			do i=1 to 52 by step;
				dt_thisyear=lastdt-7*52+7*i;
				dt_yearago=lastdt-7*104+7*i;
				startwk1=substr(compress(strip(put(dt_thisyear,weekv9.)),"W"),1,6);
				endwk1=substr(compress(strip(put(dt_thisyear+7*(step-1),weekv9.)),"W"),1,6);
				startwk0=substr(compress(strip(put(dt_yearago,weekv9.)),"W"),1,6);
				endwk0=substr(compress(strip(put(dt_yearago+7*(step-1),weekv9.)),"W"),1,6);
				periodname1=upcase(substr(periodicity,1,1))||strip(ifc(step=1,strip(startwk1),strip(startwk1)||"_"||strip(endwk1)));
				periodname0=upcase(substr(periodicity,1,1))||strip(ifc(step=1,strip(startwk0),strip(startwk0)||"_"||strip(endwk0)));
				periodcount+1;
				output;
			end;
		end;
		if index(upcase("&periodgroups."),"M")>0 then do;
			lastmonthdt=intnx('month',lastdt,0,"END");
			if lastmonthdt-lastdt>0 then lastmonthdt=intnx('month',lastdt,-1,"END");
			firstdt=intnx('month',lastmonthdt+1,-24,"BEGINNING");
			periodicity="M";
			keepdate="Y";
			do i=1 to 12;
					dt_thisyear=intnx('month',lastmonthdt,-12+i,"END");
					endwk1=substr(compress(put(dt_thisyear,weekv9.),"W"),1,6);
					startwk1=substr(compress(put(intnx('month',dt_thisyear,0,"BEGINNING"),weekv9.),"W"),1,6);
					endwk0=substr(compress(put(intnx('month',dt_thisyear,-12,"END"),weekv9.),"W"),1,6);
					startwk0=substr(compress(put(intnx('month',dt_thisyear,-12,"BEGINNING"),weekv9.),"W"),1,6);
					mon=month(dt_thisyear);
					periodname1="MON_"||strip(put(year(dt_thisyear)  ,4.))||"_"||strip(put(mon,z2.));
					periodname0="MON_"||strip(put(year(dt_thisyear)-1,4.))||"_"||strip(put(mon,z2.));
					periodcount+1;
					output;
			end;
		end;
		call symput('firstweek',substr(compress(put(firstdt,weekv9.),"W"),1,6));
		call symput('nperiods',compress(put(periodcount,8.)));
		call symput('keepdate',strip(keepdate));
	run;

	%put firstweek-lastweek=&firstweek.-&lastweek. nperiods=&nperiods.;
	%create_prodfilter_format(&use_prodgrouplist.,&prodgrouplist_filepath.,prcat);
	%create_customer_segments(&lastweek.,&segments.,numbered_names=Y);
	%get_weekly_transactions(&firstweek.,&lastweek.,&groups.,nseg=&nsegments.,use_filter=&use_prodgrouplist.,keepdt=&keepdate.);

	%do p=1 %to &nperiods.;

		data _null_;
			set periods(obs=&p. firstobs=&p.);
			call symput('startwk0',strip(startwk0));
			call symput('endwk0',strip(endwk0));
			call symput('startwk1',strip(startwk1));
			call symput('endwk1',strip(endwk1));
			call symput('periodicity',strip(periodicity));
			call symput('periodname1',strip(periodname1));
			if periodicity="M" then call symput('mon',strip(put(mon,3.)));
		run;

		%put ### &periodicity. &periodname1.: &startwk0.-&endwk0. &startwk1.-&endwk1.;

		%if &periodicity.^=W %then %do;
			%do i=0 %to 1;
				%let nw=%week_array(&&startwk&i.,&&endwk&i.);
				%let _timer_start = %sysfunc(datetime());
				data noncard_item_&periodicity._&&endwk&i.;
					set	%do w=1 %to &nw.; noncard_item_W_&&week&w. %end;;
					%if &periodicity.=M %then where month(date)=&mon.;;
					by bskid;
				run;
	 			data card_item_&periodicity._&&endwk&i.;
					set	%do w=1 %to &nw.; card_item_W_&&week&w. %end;;
					%if &periodicity.=M %then where month(date)=&mon.;;
					by hhdid bskid;
				run;
				%put Duration prepare period &periodicity. &&startwk&i.-&&endwk&i. : %sysfunc(putn(%sysevalf(%sysfunc(datetime()) - &_timer_start),time13.2));
			%end;
		%end;

		%do a=1 %to &naggregations.;
			data _null_;
				set aggregations(obs=&a. firstobs=&a.);
				call symput('class',strip(class));
			run;
			%put --- a=&a. class=&class.;

			%let _timer_start = %sysfunc(datetime());
			%sum_noncard(noncard_item_&periodicity._&endwk0.,&class.,noncard_sum0);
			%sum_card(card_item_&periodicity._&endwk0.,&class.,&nsegments.,card_sum0);
			%put Duration sum  &periodicity. &startwk0.-&endwk0. it&a. &class. : %sysfunc(putn(%sysevalf(%sysfunc(datetime()) - &_timer_start),time13.2));
			%let _timer_start = %sysfunc(datetime());
			%sum_noncard(noncard_item_&periodicity._&endwk1.,&class.,noncard_sum1);
			%sum_card(card_item_&periodicity._&endwk1.,&class.,&nsegments.,card_sum1);
			%put Duration sum &periodicity. &startwk1.-&endwk1. it&a. &class. : %sysfunc(putn(%sysevalf(%sysfunc(datetime()) - &_timer_start),time13.2));

			%do c=0 %to 1;
				data card_sum&c.;
					set card_sum&c.;
					format tothhd 8.;
				run;
			%end;

			data periodsum_p&p._a&a.;
				if _n_=1 then set periods(obs=&p. firstobs=&p. keep=periodicity periodname1 periodname0);
				if _n_=1 then set aggregations(obs=&a. firstobs=&a. keep=_type_ chartype);
				merge	card_sum1(rename=(vis=vis1 val=val1 qty=qty1 hhd=hhd1 tothhd=tothhd1))
						noncard_sum1(rename=(vis=noncard_vis1 val=noncard_val1 qty=noncard_qty1))
						card_sum0(rename=(vis=vis0 val=val0 qty=qty0 hhd=hhd0 tothhd=tothhd0))
						noncard_sum0(rename=(vis=noncard_vis0 val=noncard_val0 qty=noncard_qty0));
				by &class. segnum segment;
			run;

			proc delete data=noncard_sum0 noncard_sum1 card_sum0 card_sum1;
			run;

		%end;

		%if &periodicity.^=W %then %do;
			proc delete data=noncard_item_&periodicity._&endwk0. noncard_item_&periodicity._&endwk1. card_item_&periodicity._&endwk0. card_item_&periodicity._&endwk1.;
			run;
		%end;

	%end;

	proc datasets nolist nowarn nodetails;
		delete noncard_item_W: card_item_W:;
	run;


	data allsum;
		attrib periodicity periodname1 _type_ chartype &groups. label="";
		set
		%do p=1 %to &nperiods.;
			%do a=1 %to &naggregations.; periodsum_p&p._a&a. %end;
		%end;
		;
		array z _numeric_;
		do over z; if z=. then z=0; end;
	run;

	proc datasets nolist nowarn nodetails;
		delete periodsum_:;
	run;

	data segtotals(keep=periodicity periodname1 segnum segment tothhd0 tothhd1);
		set allsum(where=(_type_=0));
		tothhd1=hhd1;
		tothhd0=hhd0;
	run;

	%description_formats;

	data data1_to_xls(drop=rc rename=(_type_=droptype segment=seg));
		attrib periodicity periodname1 periodname0 _type_ chartype label="";
		%do i=1 %to &ngroups.; format %scan(&groups.,&i.) $100.; %end;
		%do i=%eval(&ngroups.+1) %to 3; dropdown&i.="ALL"; %end;
		set allsum;
		if _n_=1 then do;
			declare hash htot(dataset:"segtotals");
			htot.definekey("periodicity","periodname1","segnum","segment");
			htot.definedata("tothhd1","tothhd0");
			htot.definedone();
		end;
		rc=htot.find();
		%do i=1 %to &ngroups.;
			%let g=%scan(&groups.,&i.);
			if substr(chartype,&i.,1)="0" then &g.="ALL";
			else &g.=strip(&g.)||ifc(missing(strip(put(&g.,$&g.ds.))),""," - ")||strip(put(&g.,$&g.ds.));
			rename &g.=dropdown&i.;
		%end;
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

	proc export dbms=xlsx replace data=data1_to_xls outfile="&exdir./trhash_&manuf.-&gnames.-&lastweek..xlsx";
	run;

%mend tracker;

%tracker(
lastweek=202048,
use_prodgrouplist=N,
manuf=EXAMPLE1,
groups=prdep prcat prbrn,
hierarchies=prdep_prcat_prbrn,
segments=ps,
periodgroups=W B F Q M);

%put FULL DURATION: %sysfunc(putn(%sysevalf(%sysfunc(datetime()) - &_global_timer_start),time13.2));
