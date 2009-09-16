--
-- TestSelfJoins.txt
--
create table tsj1 (a integer primary key, b integer);
insert into tsj1 values(5,5);
insert into tsj1 values(6,6);
insert into tsj1 values(11,21);
insert into tsj1 values(12,22);
insert into tsj1 values(13,23);
insert into tsj1 values(14,24);
insert into tsj1 values(15,25);
insert into tsj1 values(16,26);
insert into tsj1 values(17,27);
/*r3*/select count(*) from tsj1 where a > 14
/*r5*/select count(*) from tsj1 where cast(a as character(2)) > '14'
/*r81*/select count(*) from tsj1 a cross join tsj1 b

/*e*/select count(*) from tsj1 a cross join tsj1 b on a.a = b.a
/*r9*/select count(*) from tsj1 a inner join tsj1 b on a.a = b.a
