/* macro variables */
%let user=SASJST;
%let password="{SAS002}1D57933958C580064BD3DCA81A33DFB2";
 
/* Access Snowflake data using SAS libname */
libname mydata snow server='saspartner.snowflakecomputing.com' user=&user password=&password
   database=USERS_DB schema=SASJST
   warehouse=USERS_WH role=developer
   readbuff=30000 insertbuff=30000
   dbcommit=10000 autocommit=no preserve_tab_names=no preserve_col_names=no;
 
/* Alternative Access Snowflake data using authdomain
libname mydata snow server='saspartner.snowflakecomputing.com' authdomain='snowflakeAuth'
   database=USERS_DB schema=SASJST
   warehouse=USERS_WH role=developer
   readbuff=30000 insertbuff=30000
   dbcommit=10000 autocommit=no preserve_tab_names=no preserve_col_names=no;
*/
 
/* Use Snowflake data in a SAS job */
title "Frequencies for Categorical Variables";
proc freq data=mydata.baseball;
   tables Team League Division Position Div / plots=(freqplot);
run;
 
title "Descriptive Statistics for Numeric Variables";
proc means data=mydata.baseball n nmiss min mean median max std;
   var nAtBat nHits nHome nRuns nRBI nBB YrMajor CrAtBat CrHits CrHome CrRuns
       CrRbi CrBB nOuts nAssts nError Salary logSalary;
run;
 
title;
proc univariate data=mydata.baseball noprint;
   histogram nAtBat nHits nHome nRuns nRBI nBB YrMajor CrAtBat CrHits CrHome
             CrRuns CrRbi CrBB nOuts nAssts nError Salary logSalary;
run;
 
/* Extract data from Snowflake into any SAS Library using Datastep */
/* SAS system option to show where processing took place */
OPTIONS sastrace=',,,dts' sastraceloc=saslog nostsuffix msglevel=i;
 
/* Datastep example */
data work.cars;
   set mydata.cars;
run;
 
/* Load data from any SAS library to Snowflake using SAS Datastep */
/* drop existing table */
proc sql;
   drop table mydata.cars;
quit;
 
/* copy a table to snowflake */
data mydata.cars;
   set sashelp.cars;
run;
 
/* Bulk Load data to Snowflake using SAS Datastep */
proc sql;
   drop table mydata.cars;
quit;
 
data mydata.cars(bulkload=yes BL_INTERNAL_STAGE='@~');
   set sashelp.cars;
run;
 
/* Bulk Unload Snowflake data into SAS using Datastep */
data work.cars;
   set mydata.CARS(bulkunload=yes BL_INTERNAL_STAGE='@~');
run;
 
/* Run Queries in Snowflake using Proc SQL Pushdown */
/* Explicit SQL passthru  */ 
%let server="saspartner.snowflakecomputing.com";
%let user=sasjst;
%let password="{SAS002}1D57933958C580064BD3DCA81A33DFB2" ;
%let schema=TPCH_SF1000_CLUSTERED;
%let database=TPCH_DB;
 
/* Base SAS Proc Pushdown into Snowflake */
proc means data=mydata.CARS;
run;
 
proc freq data=mydata.CARS;
run;
 
/* Run Queries in Snowflake using Proc SQL Pushdown */
/* Explicit SQL passthru  */
proc sql;
   connect to snow as myconn (server=&server user=&user password=&password schema=&schema database=&database);
       select * from connection to myconn
         (
          select count(*) from lineitem
         );
   disconnect from myconn;
quit;
 
proc sql;
   connect to snow as myconn (server=&server user=&user password=&password schema=&schema database=&database);
       select * from connection to myconn
       (select
       l_returnflag,
       l_linestatus,
       sum(l_quantity) as sum_qty,
       sum(l_extendedprice) as sum_base_price,
       sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
       sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
       avg(l_quantity) as avg_qty,
       avg(l_extendedprice) as avg_price,
       avg(l_discount) as avg_disc,
       count(*) as count_order
       from
         lineitem
       where
         l_shipdate <= dateadd(day, -90, to_date('1998-12-01'))
       group by
         l_returnflag,
         l_linestatus
       order by
         l_returnflag,
         l_linestatus
         );
   disconnect from myconn;
quit;
 