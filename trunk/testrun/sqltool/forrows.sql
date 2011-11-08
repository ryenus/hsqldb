create table t(i int, vc varchar(25));
insert into t values(1, 'one');
insert into t values(2, 'two');
insert into t values(null, 'two');
insert into t values(4, null);

*forrows INT VCHAR
select * from t;
    \p Q1 *{*ROW}
    \p INT=*{:INT}
    \p VC=(*{:VCHAR})
*end forrows
