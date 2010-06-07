-- field limit tests
-- works fully with sql.enforce_strict_size
-- from 1.8.0.RC9 DECIMAL precision and scale is enforced
-- when scale is specified, all input is rounded down to the relevant scale
-- precision is then enforced on the result of rounding down
drop table flimit if exists;
create table flimit (charf1 character(4), vcharf1 varchar(4), decf1 decimal(10,2));
insert into flimit values('one', 'two', 10.2);
/*r
 one ,two,10.20
*/select * from flimit;
/*u1*/delete from flimit;
-- to check
/*e*/insert into flimit values('twenty', 'two', 10.2);
/*e*/insert into flimit values('one', 'twenty', 10.2);
insert into flimit values('one', 'two', 99999999.11111);
/*r
 one ,two,99999999.11
*/select * from flimit;
/*r
 one ,two,99999999.11
*/select * from flimit where charf1 in('one','two');
/*r
 one ,two,99999999.11
*/select * from flimit where charf1 in('one   ','two');
/*r
 99999999
*/select cast(decf1 as numeric(8)) from flimit;
/*r
 99999999.1100
*/select cast(decf1 as numeric(12,4)) from flimit;
/*r
 one tw,tw
*/select cast((charf1 || vcharf1) as varchar(6)), cast(vcharf1 as varchar(2)) from flimit;
/*e*/insert into flimit values('one', 'two', 999999999.11);
/*e*/insert into flimit values('one', 'two', cast(999999999.11111 as decimal(10,2)));
insert into flimit values('one', 'two', cast(99999999.11111 as decimal(10,2)));
drop table flimit;
create table flimit (a timestamp(0), b timestamp(6), c timestamp);
insert into flimit values('2005-05-21 10:10:10.123456','2005-05-21 10:10:10.123456','2005-05-21 10:10:10.123456');
/*c0*/select * from flimit where a = b;
/*c0*/select * from flimit where a = c;
/*c1*/select * from flimit where b = c;
--
drop table flimit if exists;
create table flimit (bit1 bit , bit3 bit(3), bitvar bit varying(3));
/*e*/insert into flimit values(B'1', B'1011', B'101');
/*e*/insert into flimit values(B'1', B'101', B'1011');
/*e*/insert into flimit values(B'10', B'101', B'1010');
/*e*/insert into flimit values(B'1', B'10', B'1010');
insert into flimit values(B'1', B'101', B'101');
/*r1101*/select bit1 || bit3 from flimit
/*r101101*/select bit3 || bitvar from flimit
