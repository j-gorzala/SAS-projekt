libname dane 'C:\Users\kubag\Desktop\projekt_SAS\dane';


/* ��czenie TRAIN i VALID w jeden zbi�r */
data ABT_SAM_BEH;
set dane.ABT_SAM_BEH_TRAIN dane.ABT_SAM_BEH_VALID indsname=name;
if scan(name,2) = 'ABT_SAM_BEH_TRAIN' then table_name = 'TRAIN';
else table_name = 'VALID';
run;

proc sort data=ABT_SAM_BEH;
	by cid period;
run;


/* _________________________________________________________________ */
/* Sprawdzanie liczebno�ci zbioru TRAIN i VALID dla kolejnych lat */
proc sql;
create table months_check as
select table_name, period, substr(period,1,4) as year, count(*) as count
from ABT_SAM_BEH
group by table_name, period
order by period, table_name
;
run;

title "Liczba obserwacji dla zbioru TRAIN i VALID dla kolejnych lat";
proc sgplot data = months_check;
    vbar year / response=count group = table_name datalabel;
	yaxis label='Liczba';
run;
/* _________________________________________________________________ */




%macro policz_braki(dataset);

	proc contents data=ABT_SAM_BEH out=colnames (keep=NAME) ; 
	run; 

	%let dsid = %sysfunc(open(colnames));
	%let num = %sysfunc(attrn(&dsid,nlobs));
	%let vnum = %sysfunc(varnum(&dsid, NAME));
	
	data braki_danych;
	set &dataset;
	%do n=1 %to &num;
		%let rc = %sysfunc(fetchobs(&dsid, &n));
		%let name = %sysfunc(getvarc(&dsid,&vnum));
		%if &name ^= cid and 
			&name ^= period and 
			&name ^= table_name 
		%then %do;
			if missing(&name) then &name = 1;
			else &name = 0;
			&name = input(&name, best12.);
		%end;
	%end;
	%let rc = %sysfunc(close(&dsid));
	drop cid app_char_job_code app_char_city 
			app_char_home_status app_char_cars;
	run;


	proc sort data=braki_danych;
		by period;
	run;


	proc sql noprint;
	select NAME 
	into :columns separated by ' '
	from colnames
	where NAME not in ('app_char_cars', 
		'app_char_city', 'app_char_home_status', 
		'app_char_job_code', 'app_char_marital_status', 
		'cid', 'period', 'table_name');
	quit;


	proc summary noprint data=braki_danych;
		class period table_name;
		var &columns;
		OUTPUT OUT=null_summary MEAN=&columns /autoname autolabel;
	run;


	data raport_nulls_by_set_period;
	set null_summary;
	if period ^= '' and table_name ^= '' then output;
	drop _TYPE_ _FREQ_;
	run;
	proc transpose data=raport_nulls_by_set_period out=raport_nulls_by_set_period_t;
   	by period table_name;
	run;
	data raport_nulls_by_set_period_t;
	set raport_nulls_by_set_period_t(rename=(_NAME_=column COL1=null_perc));
	variable_group =  substr(column, 1, 3);
	year = substr(period, 1, 4);
	if substr(period, 5, 2) = 01 or substr(period, 5, 2) = 02 or substr(period, 5, 2) = 03 then quarter='Q1';
	else if substr(period, 5, 2) = 04 or substr(period, 5, 2) = 05 or substr(period, 5, 2) = 06 then quarter='Q2';
	else if substr(period, 5, 2) = 07 or substr(period, 5, 2) = 08 or substr(period, 5, 2) = 09 then quarter='Q3';
	else if substr(period, 5, 2) = 10 or substr(period, 5, 2) = 11 or substr(period, 5, 2) = 12 then quarter='Q4';
	year_quarter = year||quarter;
	drop _LABEL_;
	run;
	proc sql;
	create table raport_nulls_by_set_period_t as
	select *,
		CASE WHEN null_perc >= 0 and null_perc <= 0.2 THEN '0%-20%'
			WHEN null_perc > 0.2 and null_perc <= 0.4 THEN '20%-40%'
			WHEN null_perc > 0.4 and null_perc <= 0.6 THEN '40%-60%'
			WHEN null_perc > 0.6 and null_perc <= 0.8 THEN '60%-80%'
			WHEN null_perc > 0.8 and null_perc <= 1 THEN '80%-100%'
		END AS null_perc_category
	from raport_nulls_by_set_period_t a
	left join dane.dictionary b
	on a.column = b.column_name
	;
	quit;


	data raport_nulls_by_set;
	set null_summary;
	if period = '' and table_name ^= '' then output;
	drop _TYPE_ _FREQ_;
	run;
	proc transpose data=raport_nulls_by_set out=raport_nulls_by_set_t;
   	by period table_name;
	run;
	data raport_nulls_by_set_t;
	set raport_nulls_by_set_t(rename=(_NAME_=column COL1=null_perc));
	variable_group =  substr(column, 1, 3);
	drop period _LABEL_;
	run;
	proc sql;
	create table raport_nulls_by_set_t as
	select *,
		CASE WHEN null_perc >= 0 and null_perc <= 0.2 THEN '0%-20%'
			WHEN null_perc > 0.2 and null_perc <= 0.4 THEN '20%-40%'
			WHEN null_perc > 0.4 and null_perc <= 0.6 THEN '40%-60%'
			WHEN null_perc > 0.6 and null_perc <= 0.8 THEN '60%-80%'
			WHEN null_perc > 0.8 and null_perc <= 1 THEN '80%-100%'
		END AS null_perc_category
	from raport_nulls_by_set_t a
	left join dane.dictionary b
	on a.column = b.column_name
	;
	quit;


	data raport_nulls_all;
	set null_summary;
	if period = '' and table_name = '' then output;
	drop _TYPE_ _FREQ_;
	run;
	proc transpose data=raport_nulls_all out=raport_nulls_all_t;
   	by period table_name;
	run;
	data raport_nulls_all_t;
	set raport_nulls_all_t(rename=(_NAME_=column COL1=null_perc));
	variable_group =  substr(column, 1, 3);
	drop period table_name _LABEL_;
	run;
	proc sql;
	create table raport_nulls_all_t as
	select *,
		CASE WHEN null_perc >= 0 and null_perc <= 0.2 THEN '0%-20%'
			WHEN null_perc > 0.2 and null_perc <= 0.4 THEN '20%-40%'
			WHEN null_perc > 0.4 and null_perc <= 0.6 THEN '40%-60%'
			WHEN null_perc > 0.6 and null_perc <= 0.8 THEN '60%-80%'
			WHEN null_perc > 0.8 and null_perc <= 1 THEN '80%-100%'
		END AS null_perc_category
	from raport_nulls_all_t a
	left join dane.dictionary b
	on a.column = b.column_name
	;
	quit;

%mend;


%policz_braki(ABT_SAM_BEH);
/* Makro zwraca nast�puj�ce tabele: */
/*RAPORT_NULLS_ALL_T - podsumowanie % brak�w danych dla obu zbior�w dla ca�ego okresu*/
/*RAPORT_NULLS_BY_SET_T - podsumowanie % brak�w danych dla obu zbior�w dla ca�ego okresu z podzia�em na zbi�r*/
/*RAPORT_NULLS_BY_SET_PERIOD_T - podsumowanie % brak�w danych dla obu zbior�w dla ca�ego okresu z podzia�em na zbi�r i okres*/


/* Sprawdzanie liczebnosci poszczegolnych klas wybranej zmiennej */
%let column_to_check = agr9_Median_Cncr;
proc sql;
create table test as
select &column_to_check, count(*) as cnt
from ABT_SAM_BEH
group by &column_to_check;
quit;




/* Punkt 1*/
/*Analiza brak�w danych, ich udzia��w w czasie */
/*(czyli stabilno�ci w czasie) i por�wnywania udzia��w */
/*pomi�dzy zbiorami train i valid (czyli stabilno�ci na zbiorach) */
/*w postaci szczeg�owego raportu tabelarycznego.*/
/* _________________________________________________________________ */
/* RAPORTY */


/* Raport 0 */
/* Zmienne celu */
proc sql;
create table report_0_data as
select table_name, column, mean(null_perc) as null_perc, year_quarter
from RAPORT_NULLS_BY_SET_PERIOD_T
where column in ('default_cus3', 'default_cus6', 'default_cus9', 'default_cus12')
group by year_quarter, table_name, column
order by column, year_quarter, table_name;
quit;
proc report data=report_0_data;
   columns year_quarter column table_name null_perc;
   define column / 'Kolumna' GROUP WIDTH=6 CENTER;
   define table_name / 'Pr�ba' GROUP WIDTH=6 CENTER;
   define null_perc / '�redni udzia� brak�w danych' GROUP WIDTH=6 CENTER;
   define year_quarter / 'Okres' GROUP WIDTH=6 CENTER;
   title 'Udzia� brak�w danych dla zmiennych celu'; 
run;




/* Raport 1 */
/* Zmienne ze 100% brakiem danych */
proc report data=RAPORT_NULLS_ALL_T;
   where null_perc=1;
   columns column_name null_perc aggregation var;
   define column_name / 'Nazwa zmiennej' GROUP WIDTH=6 CENTER;
   define null_perc / 'Udzia� brak�w' GROUP WIDTH=6 CENTER f=percent9.3;
   define aggregation / 'Agregacja' GROUP WIDTH=6 CENTER;
   define var / 'Kategoria zmiennej' GROUP WIDTH=6 CENTER;
   title 'Zmienne z najwi�kszym udzia�em brak�w danych'; 
run;


/* Raport 2 */
/* Zmienne ze 100% brakiem danych - liczba wzgl�dem agregacji i kategorii */
data report_2_data_1;
set RAPORT_NULLS_ALL_T;
where null_perc = 1;
run;
proc sql;
create table report_2_data_2 as
select aggregation, var, count(*) as count 
from report_2_data_1
group by aggregation, var;
quit;
proc report data=report_2_data_2;
   columns aggregation var count;
   define aggregation / 'Agregacja' GROUP WIDTH=6 CENTER;
   define var / 'Kategoria zmiennej' GROUP WIDTH=6 CENTER;
   define count / '��czna liczba zmiennych' GROUP WIDTH=6 CENTER;
   title 'Liczba zmiennych ze 100% brakiem danych wzgl�dem kategorii'; 
run;


/* Raport 3 */
/* Liczba zmiennych wzgl�dem �r�d�a danych, grupy i przedzia�u udzia�u brak�w */
proc sql;
create table report_3_data as
select table_name, null_perc_category, count(*) as count 
from RAPORT_NULLS_BY_SET_T
group by null_perc_category, table_name;
quit;
proc report data=report_3_data;
   columns table_name null_perc_category count;
   define table_name / 'Pr�ba' GROUP WIDTH=6 CENTER;
   define null_perc_category / 'Udzia� brak�w danych' GROUP WIDTH=6 CENTER;
   define count / '��czna liczba zmiennych' GROUP WIDTH=6 CENTER;
   title 'Liczba zmiennych wzgl�dem pr�by i udzia�u brak�w danych'; 
run;


/* Raport 4 */ 
/* Zmienne o najwi�kszej zmienno�ci udzia�u brak�w danych */
proc sql;
create table report_4_data_1 as
select column, std(null_perc) as null_perc_std /*max(null_perc) - min(null_perc) as null_perc_diff*/
from RAPORT_NULLS_BY_SET_PERIOD_T
group by column
order by calculated null_perc_std desc;
quit;
proc sql;
create table report_4_data_2 as
select * from report_4_data_1 (OBS=20);
quit;
proc report data=report_4_data_2;
   columns column null_perc_std;
   define column / 'Kolumna' GROUP WIDTH=6 CENTER;
   define null_perc_std / 'Odchylenie standardowe udzia�u brak�w danych' GROUP WIDTH=6 CENTER;
   title 'TOP20 zmiennych o najwi�kszej zmienno�ci udzia�u brak�w danych na przestrzeni czasu obserwacji'; 
run;


/* Raport 5 */
/* �redni udzia� brak�w danych na wszystkich zmiennych wzgl�dem pr�by po latach */
proc sql;
create table report_5_data as
select table_name, year, mean(null_perc) as mean_null_perc 
from RAPORT_NULLS_BY_SET_PERIOD_T
group by table_name, year;
quit;
proc report data=report_5_data;
   columns year table_name mean_null_perc;
   define year / 'Rok obserwacji' GROUP WIDTH=6 CENTER;
   define table_name / 'Pr�ba' GROUP WIDTH=6 CENTER;
   define mean_null_perc / '�redni udzia� brak�w danych' GROUP WIDTH=6 CENTER;
   title '�redni udzia� brak�w danych wzgl�dem pr�by i roku obserwacji'; 
run;


/* Raport 6 */
/* �redni udzia� brak�w danych wzgl�dem grupy zmiennych i pr�by */
proc sql;
create table report_6_data as
select table_name, variable_group, mean(null_perc) as null_perc_mean 
from RAPORT_NULLS_BY_SET_T
group by variable_group, table_name;
quit;
proc report data=report_6_data;
   columns variable_group table_name null_perc_mean;
   define variable_group / 'Grupa zmiennej' GROUP WIDTH=6 CENTER;
   define table_name / 'Pr�ba' GROUP WIDTH=6 CENTER;
   define null_perc_mean / '�redni udzia� brak�w danych' GROUP WIDTH=6 CENTER;
   title '�redni udzia� brak�w danych wzgl�dem grupy zmiennych i pr�by'; 
run;


/* Raport 7 */
/* �redni udzia� brak�w danych wzgl�dem rodzaju statystyki */
proc sql;
create table report_7_data as
select stat, statc, mean(null_perc) as null_perc_mean 
from RAPORT_NULLS_BY_SET_T
group by stat, statc;
quit;
proc report data=report_7_data;
   columns stat statc null_perc_mean;
   define stat / 'Statystyka' GROUP WIDTH=6 CENTER;
   define statc / 'Max/Min/Sum' GROUP WIDTH=6 CENTER;
   define null_perc_mean / '�redni udzia� brak�w danych' GROUP WIDTH=6 CENTER;
   title '�redni udzia� brak�w danych wzgl�dem rodzaju statystyki'; 
run;


/* Raport 8 */
/* �redni udzia� brak�w danych wzgl�dem kwarta�u i pr�by */
proc sql;
create table report_8_data as
select quarter, table_name, mean(null_perc) as null_perc_mean 
from RAPORT_NULLS_BY_SET_PERIOD_T
group by quarter, table_name;
quit;
proc report data=report_8_data;
   columns quarter table_name null_perc_mean;
   define quarter / 'Kwarta�' GROUP WIDTH=6 CENTER;
   define table_name / 'Pr�ba' GROUP WIDTH=6 CENTER;
   define null_perc_mean / '�redni udzia� brak�w danych' GROUP WIDTH=6 CENTER;
   title '�redni udzia� brak�w danych wzgl�dem rodzaju statystyki'; 
run;





/* _________________________________________________________________ */
/* WYKRESY */
/* Punkt 2*/
/*Analiza brak�w danych w postaci zwizualizowanych graficznych raport�w */
/*w celu szybkiego identyfikowania zmiennych z wi�ksz� i mniejsz� */
/*liczb� brak�w danych oraz z ich r�n� stabilno�ci�.*/


/* Wykres 0 - rozk�ad brak�w danych w czasie (po kolejnych kwarta�ach) - mo�na zadeklarowa� dowolne zmienne (wpisa� w %let) - agregacja po zbiorach */
%let plot_0_variables = ('default_cus3', 'default_cus6', 'default_cus9', 'default_cus12');
proc sql;
create table plot_0_data as
select column, mean(null_perc) as null_perc, year_quarter
from RAPORT_NULLS_BY_SET_PERIOD_T
where column in &plot_0_variables.
group by year_quarter, column
order by column, year_quarter;
quit;
proc sgplot data = plot_0_data;
series x=year_quarter y=null_perc / group=column;
	xaxis label="Kwarta�" labelattrs=(size=12) valueattrs=(size=5);
	yaxis label="Udzia� brak�w danych" labelattrs=(size=12) valueattrs=(size=10);
	keylegend / title='Zmienna';
 title 'Udzia� brak�w danych dla wybranych zmiennych';
run;


/* Wykres 1 */
title "�redni udzia� brak�w danych dla zbioru TRAIN i VALID dla kolejnych lat";
proc sgplot data = report_5_data;
    vbar year / response=mean_null_perc group = table_name groupdisplay=cluster
	datalabel;
	yaxis grid label='�redni udzia� brak�w danych';
	keylegend / title='Pr�ba';
	xaxis grid label='Rok';
run;


/* Wykres 2 */
title "�redni udzia� brak�w danych dla zbioru TRAIN i VALID dla kwarta��w";
proc sgplot data = report_8_data;
    vbar quarter / response=null_perc_mean group = table_name groupdisplay=cluster
	datalabel;
	yaxis grid label='�redni udzia� brak�w danych';
	xaxis grid label='Kwarta�';
	keylegend / title='Pr�ba';
run;


/* Wykres 3 */
title "�redni udzia� brak�w danych dla zbioru TRAIN i VALID dla grup zmiennych";
proc sgplot data = report_6_data;
    vbar variable_group / response=null_perc_mean group = table_name groupdisplay=cluster
	datalabel;
	yaxis grid label='�redni udzia� brak�w danych';
	xaxis grid label='Grupa zmiennej';
	keylegend / title='Pr�ba';
run;


/* Wykres 4 */
proc sql;
create table plot_4_data as
select year, variable_group, mean(null_perc) as null_perc_mean
from RAPORT_NULLS_BY_SET_PERIOD_T
group by year, variable_group;
quit;
title "�redni udzia� brak�w danych dla grup zmiennych na przestrzeni lat";
proc sgplot data = plot_4_data;
    vbar year / response=null_perc_mean group = variable_group groupdisplay=cluster
	datalabel;
	yaxis grid label='�redni udzia� brak�w danych';
	xaxis grid label='Rok';
	keylegend / title='Grupy zmiennych';
run;


/* Wykres 5 */
proc sql;
create table plot_5_data_1 as
select year, count(column) as colname_year_cnt
from RAPORT_NULLS_BY_SET_PERIOD_T
group by year;
quit;
proc sql;
create table plot_5_data_2 as
select distinct a.year, null_perc_category, count(column)/colname_year_cnt as count_perc
from RAPORT_NULLS_BY_SET_PERIOD_T a
left join plot_5_data_1 b
	on a.year = b.year
group by a.year, null_perc_category
order by a.year, null_perc_category;
quit;
title "Udzia� zmiennych wzgl�dem procentowego przedzia�u brak�w danych na przestrzeni lat";
proc sgplot data = plot_5_data_2;
    vbar year / response=count_perc group = null_perc_category groupdisplay=cluster
	datalabel;
	yaxis grid label='Udzia� zmiennych';
	xaxis grid label='Rok';
	keylegend / title='Procentowy przedzia� brak�w danych dla zmiennej';
run;


/* Wykres 6 */
/* Wykres dowolnej wskazanej zmiennej - wykres punktowy */
%let zmienna_wykres = default_cus6;
title "Udzia� brak�w danych dla zmiennej '&zmienna_wykres.' dla kolejnych okres�w";
proc sgplot data = RAPORT_NULLS_BY_SET_PERIOD;
    scatter x=period y=&zmienna_wykres / group = table_name;
	yaxis grid label='Udzia� brak�w danych';
	xaxis grid display=none;
	keylegend / title='Pr�ba';
run;