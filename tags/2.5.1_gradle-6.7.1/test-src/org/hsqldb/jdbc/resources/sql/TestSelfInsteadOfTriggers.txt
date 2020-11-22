drop table testtrig12 if exists;
drop table testtrig13 if exists;
create cached table testtrig12(id integer, data2 varchar(20), updated date);
create cached table testtrig13(id integer, data3 varchar(20), op varchar(10));
create view viewinst (vid, vdata2, vdata3) as select id, data2, data3 from testtrig12 natural join testtrig13
create trigger trigger2 instead of insert on viewinst
 referencing new row as newrow
 for each row
 begin atomic
 insert into testtrig12 values(newrow.vid, newrow.vdata2, current_date);
 insert into testtrig13 values (newrow.vid, newrow.vdata3, 'inserted');
 end

create trigger trigger3 instead of delete on viewinst
 referencing old row as oldrow
 for each row
 begin atomic
 delete from testtrig12 where testtrig12.id = oldrow.vid;
 delete from testtrig13 where testtrig13.id = oldrow.vid;
 end

create trigger trigger14 instead of update on viewinst
 referencing old row as oldrow new row as newrow
 for each row
 begin atomic
 update testtrig12 set data2 = newrow.vdata2, updated = current_date where testtrig12.id = oldrow.vid;
 update testtrig13 set data3 = newrow.vdata3, op = 'updated' where testtrig13.id = oldrow.vid;
 end

insert into viewinst values (1, 'data2 value1', 'data3 value1')
insert into viewinst values (2, 'data2 value2', 'data3 value2')

/*r
 1,data3 value1,inserted
 2,data3 value2,inserted
*/select * from testtrig13

update viewinst set vdata2='data2 updated1', vdata3='data3 updated1' where vid=1

delete from viewinst
