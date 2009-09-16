--
-- TestSelfQueries.txt
--
-- Tests for Query results, especially the neutrality of constraints and indexes
--
drop table TESTTABLE if exists;

create cached table TESTTABLE (
    aString              varchar(256)                   not null,
    firstNum             integer                        not null,
    aDate                date                           not null,
    secondNum            integer                        not null,
    thirdNum             integer                        not null,
    aName                varchar(32)                    not null
  );
create  index IDX_TESTTABLE_aString on TESTTABLE (aString);
create  index IDX_TESTTABLE_aDate_secondNum on TESTTABLE (aDate,secondNum);
insert into TESTTABLE(aString, firstNum, aDate, secondNum, thirdNum, aName)
  values ('Current', 22, '2003-11-10', 18, 3, 'my name goes here');
insert into TESTTABLE(aString, firstNum, aDate, secondNum, thirdNum, aName)
  values ('Popular', 23, '2003-11-10', 18, 3, 'my name goes here');
insert into TESTTABLE(aString, firstNum, aDate, secondNum, thirdNum, aName)
  values ('New', 5, '2003-11-10', 18, 3, 'my name goes here');
insert into TESTTABLE(aString, firstNum, aDate, secondNum, thirdNum, aName)
  values ('Old', 5, '2003-11-10', 18, 3, 'my name goes here');
insert into TESTTABLE(aString, firstNum, aDate, secondNum, thirdNum, aName)
  values ('CCurrent', 5, '2003-11-10', 18, 3, 'my name goes here');
insert into TESTTABLE(aString, firstNum, aDate, secondNum, thirdNum, aName)
  values ('ELV', 5, '2003-11-10', 18, 3, 'my name goes here');
insert into TESTTABLE(aString, firstNum, aDate, secondNum, thirdNum, aName)
  values ('ELNA', 5, '2003-11-10', 18, 3, 'my name goes here');
insert into TESTTABLE(aString, firstNum, aDate, secondNum, thirdNum, aName)
  values ('Older', 5, '2003-11-10', 18, 3, 'my name goes here');
insert into TESTTABLE(aString, firstNum, aDate, secondNum, thirdNum, aName)
  values ('RA', 20, '2003-11-10', 18, 3, 'my name goes here');
insert into TESTTABLE(aString, firstNum, aDate, secondNum, thirdNum, aName)
  values ('RP', 2, '2003-11-10', 18, 3, 'my name goes here');
insert into TESTTABLE(aString, firstNum, aDate, secondNum, thirdNum, aName)
  values ('VS', 3, '2003-11-10', 18, 3, 'my name goes here');
--
/*c11*/select * from testtable where adate = '2003-11-10' and secondNum = 18;
/*c11*/select * from testtable where (adate, secondNum) = (DATE'2003-11-10', 18);
/*c11*/select * from testtable where adate = '2003-11-10';
/*c1*/select * from testtable where adate = '2003-11-10' and firstNum = 20;
/*c1*/select * from testtable where (adate, firstNum) = (DATE'2003-11-10', 20);
/*c11*/select * from testtable where adate = '2003-11-10' and thirdNum = 3;
/*c11*/select * from testtable where (adate, thirdNum) = (DATE'2003-11-10', 3);

drop index IDX_TESTTABLE_aString;
drop index IDX_TESTTABLE_aDate_secondNum;
alter table TESTTABLE add constraint tt_pk primary key (astring);
/*c11*/select * from testtable where adate = '2003-11-10' and secondNum = 18;
/*c11*/select * from testtable where (adate, secondNum) = (DATE'2003-11-10', 18);
/*c11*/select * from testtable where adate = '2003-11-10';
/*c1*/select * from testtable where adate = '2003-11-10' and firstNum = 20;
/*c11*/select * from testtable where adate = '2003-11-10' and thirdNum = 3;
alter table testtable drop column aname;
/*c1*/select * from testtable where adate = '2003-11-10' and firstNum = 20;
/*c11*/select * from testtable where adate = '2003-11-10' and thirdNum = 3;
alter table testtable add column aname char(10) default 'a string' not null;
/*c1*/select * from testtable where adate = '2003-11-10' and firstNum = 20;
/*c11*/select * from testtable where adate = '2003-11-10' and thirdNum = 3;
/*e*/update testtable set name=null;

-- bug #722443
DROP TABLE CONFIGUSER IF EXISTS;
CREATE CACHED TABLE CONFIGUSER(USR_USERID NUMERIC NOT NULL PRIMARY KEY,USR_USERNAME VARCHAR(10) NOT NULL,USR_PASSWORD VARCHAR(10));
INSERT INTO CONFIGUSER VALUES(-5,'guest','guest');
INSERT INTO CONFIGUSER VALUES(-4,'user','user');
INSERT INTO CONFIGUSER VALUES(-3,'owner','owner');
INSERT INTO CONFIGUSER VALUES(-2,'admin','xxx');
INSERT INTO CONFIGUSER VALUES(-1,'sadmin','xxx');
INSERT INTO CONFIGUSER VALUES(0,'nobody',null);
-- select all users with their username as password
/*c3*/select * from configuser where usr_username = usr_password;
-- create a unique index on one column
CREATE UNIQUE INDEX IDX_USERNAME ON CONFIGUSER(USR_USERNAME);
-- select all users with their username as password
/*c3*/select * from configuser where usr_username = usr_password;
/*c3*/select * from configuser where usr_username in (select usr_password from configuser)
/*c3*/select * from configuser where usr_password in (select usr_username from configuser)
/*c3*/select * from configuser where usr_password in (usr_username);
/*c2*/select * from configuser where usr_password in ('guest', 'user', 'wrong')
DROP INDEX IDX_USERNAME
-- select all users with their username as password
/*c3*/select * from configuser where usr_username = usr_password;
/*c3*/select * from configuser where usr_username in (select usr_password from configuser)
/*c3*/select * from configuser where usr_password in (select usr_username from configuser)
/*c3*/select * from configuser where usr_password in (usr_username);
/*c2*/select * from configuser where usr_password in ('guest', 'user', 'wrong')
CREATE INDEX IDX_USERNAME ON CONFIGUSER(USR_USERNAME);
-- select all users with their username as password
/*c3*/select * from configuser where usr_username = usr_password;
/*c3*/select * from configuser where usr_username in (select usr_password from configuser)
/*c3*/select * from configuser where usr_password in (select usr_username from configuser)
/*c3*/select * from configuser where usr_password in (usr_username);
/*c2*/select * from configuser where usr_password in ('guest', 'user', 'wrong')
--
-- COUNT(DISTINCT ) when there are no records
-- bug #718866
CREATE TABLE IBANX_PERMIT(ID INT, A1 VARCHAR(10));
/*r0*/SELECT count(distinct A0.ID) FROM IBANX_PERMIT A0;
--
-- use of column aliases in the where clause
--bug #696595
CREATE TABLE "liste"("kosten" INT, "preis" INT);
insert into "liste" values(100, 10);
/*r100*/SELECT "kosten" AS "total" FROM "liste" WHERE "kosten" > 0 ORDER BY "total" DESC;
/*r110*/SELECT ("kosten"+"preis") AS "total" FROM "liste" WHERE "kosten"+"preis" > 0 ORDER BY "total" DESC;
drop table t1 if exists;
drop table t2 if exists;
create table t1(t1c1 int, t1c2 int);
create table t2(t2c1 int, t2c2 int);
insert into t1 values 1, 2;
insert into t2 values 2, 3;
/*r1,2,2,3*/select * from t1, t2;
/*r1,2,2,3*/select * from t1, t2 where t1c2 = t2c1;
/*r1,2,2,3*/select * from t1 join t2 on t1c2 = t2c1;
/*r1,2,2,3*/select * from t1 left outer join t2 on t1c2 = t2c1;
/*r1,2,2,3*/select * from t1 right outer join t2 on t1c2 = t2c1;
