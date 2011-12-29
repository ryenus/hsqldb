--
-- TestSelfCreate.txt
--

-- TestSelfCreate.txt is used by TestSelf.java to test the database
--
-- This is part of a three part suite of scripts to test persistence in the same DB
--
-- Comment lines must start with -- and are ignored
-- Lines starting with spaces belongs to last line
-- Checked lines start with comments containing <tag> where <tag> is:
--   c <rows>     ResultSet expects a with <row> columns
--   r <string>   ResultSet expected with <string> result in first row/column
--   u <count>    Update count <count> expected
--   e            Exception must occur

-- Correct handling of index creation for foreign keys

create cached table VEREIN
   (
   VCODE CHAR(10) not null,
   primary key (VCODE)
   );

-- a no-op
create unique index VEREIN_PK on VEREIN (VCODE);
create cached table BEWERB
   (
   VCODE CHAR(10) not null,
   ID SMALLINT not null ,
   primary key (ID)
   );

-- a no-op
create unique index BEWERB_FK2 on BEWERB(ID);
-- a no-op
create unique index BEWERB_FK1 on BEWERB(VCODE);
alter table BEWERB add constraint bv foreign key (VCODE) references VEREIN(VCODE);

-- forward FK with index

CREATE Cached TABLE t1(
   ID INTEGER NOT NULL PRIMARY KEY,
   t2ref INTEGER
   );

CREATE Cached TABLE t2(
   ID INTEGER NOT NULL PRIMARY KEY
   );
CREATE INDEX idx on t1(t2ref);
ALTER TABLE t1 ADD CONSTRAINT fk1 FOREIGN KEY (t2ref) REFERENCES t2(ID);

-- forward FK with no index

CREATE Cached TABLE tt1(
   ID INTEGER NOT NULL PRIMARY KEY,
   tt2ref INTEGER
   );

CREATE Cached TABLE tt2(
   ID INTEGER NOT NULL PRIMARY KEY
   );
ALTER TABLE tt1 ADD CONSTRAINT fk2 FOREIGN KEY (tt2ref) REFERENCES tt2(ID);

-- column default values - scripting test

CREATE TABLE TDEF1 (
   I1 IDENTITY, C1 CHAR DEFAULT '-',
   C2 CHAR(10) DEFAULT 'hsqldb',
   I3 INTEGER DEFAULT -1,
   D4 DOUBLE DEFAULT -0.3e-5,
   T5 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   T6 TIME DEFAULT current_time,
   D7 DATE DEFAULT current_date,
   D8 VARCHAR(10) DEFAULT current_user
   );
CREATE TABLE TDEF2 (
   B1 BOOLEAN DEFAULT TRUE,
   B2 BOOLEAN DEFAULT 'false',
   T5 TIMESTAMP DEFAULT '1999-12-01 21:30:00',
   T6 TIME DEFAULT '23:12:01',
   T7 DATE DEFAULT '2002-02-15'
   );


INSERT INTO TDEF1 (I3) VALUES (0);
INSERT INTO TDEF1 (D4) VALUES (0);
INSERT INTO TDEF2 (B1) VALUES (FALSE);
INSERT INTO TDEF2 (T7) VALUES (NULL);


--bug #824031
--scripting test for order of indexes
CREATE CACHED TABLE APP (
 VARIANT_ID INTEGER NOT NULL,
 APP_ID INTEGER NOT NULL,
 APP_NAME VARCHAR (35) NOT NULL,
 CONSTRAINT PK_APP PRIMARY KEY( VARIANT_ID));
CREATE INDEX APP ON APP(APP_ID);
ALTER TABLE APP ADD CONSTRAINT APP_IX1 UNIQUE( APP_NAME);
INSERT INTO APP VALUES (1, 1, 'Shelly');
INSERT INTO APP VALUES (2, 2, 'Eran');
/*c1*/SELECT * FROM APP WHERE APP_NAME = 'Shelly';

--test identity increment
CREATE CACHED TABLE APP2 (
 ID BIGINT NOT NULL IDENTITY,
 VALUE INTEGER NOT NULL)

INSERT INTO APP2 (VALUE) VALUES(10);

--test update with no primary key
DROP TABLE FILE IF EXISTS;
CREATE TABLE FILE(ID VARCHAR(10), NAME VARCHAR(10),
 DESCRIPTION VARCHAR(10), FIELD1 INT, FIELD2 VARCHAR(10));
/*c0*/select * from file;
insert into file(id, name) values('14', 'dir');
/*c1*/select * from file;
update file set name = 'newdir' where id = '14';
/*r
  14,newdir,NULL,NULL,NULL
*/select * from file;

--test TEMP TABLE

create TEMPORARY TABLE TTEMP(C CHAR) ON COMMIT PRESERVE ROWS;
create TEMPORARY TABLE TTEMP2(C CHAR) ON COMMIT DELETE ROWS;

--test VIEW persistence with quoted column names

create table "TView" ("First Column" int, "Second Column" bigint);

create view "View" ("Id", "The Name", "The Description")
  as select ID,NAME,DESCRIPTION FROM FILE;

create view "View2"
  as select ID,NAME "The Name",DESCRIPTION "The Description" FROM FILE;

create view "View3" as select "First Column", "Second Column" from "TView";

--test inline column check constraints

create table TCONST (COLA INT UNIQUE CHECK(COLA < 10), COLB INT UNIQUE CHECK(COLB > 10))

--test separate identity and primary key

create table TIDENT (COLA INT PRIMARY KEY,
 COLB INT GENERATED BY DEFAULT AS IDENTITY(INCREMENT BY 1 START WITH 0))
insert into tident values (10, default);
insert into tident values (11, default);
insert into tident values (12, default);
/*e*/insert into tident values (10, default);
/*r
 10,0
 11,1
 12,2
*/select * from tident

-- SHUTDOWN is necessary for test1

SHUTDOWN;
