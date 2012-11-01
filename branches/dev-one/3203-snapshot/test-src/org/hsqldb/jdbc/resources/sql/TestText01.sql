-- all_quoted
drop table tmain if exists;
create table tmain(c1 varchar(10),i1 integer, c2 varchar(10));
insert into tmain values(null, null, null);
insert into tmain values('aval1', null, 'aval2');
drop table t if exists;
create text table t(c1 varchar(10),i1 integer, c2 varchar(10));
set table t source "t.txt; all_quoted=true;ignore_first=true";
set table t source header """charcol_1"",""intcol"",""charcol_2""";
insert into t select * from tmain;
-- unquoted
create table ttmain(doublequote char(10), singlequote char(10));
insert into ttmain values('noquoteA', 'noquoteB');
insert into ttmain values('"inch','''foot');
insert into ttmain values('_""_','_''''_' );
insert into ttmain values('"quoted"','''quoted''');
insert into ttmain values('inch"','foot''');
insert into ttmain values('"inch"','''foot''');
create text table tt(doubquote char(10), singquote char(10));
set table tt source "tt.txt;quoted=false;fs=\t;ignore_first=true";
set table tt source header "DOUBQUOTE,SINGQUOTE";
insert into tt select * from ttmain;
shutdown


