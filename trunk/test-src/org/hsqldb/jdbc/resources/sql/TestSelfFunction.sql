-- AUXILIAR TABLES + DECLARES
DROP TABLE tt1 IF EXISTS;
DROP TABLE tt2 IF EXISTS;
CREATE Cached TABLE tt1(
   ID INTEGER NOT NULL PRIMARY KEY,
   tt2ref INTEGER
   );

CREATE Cached TABLE tt2(
   ID INTEGER NOT NULL PRIMARY KEY
   );
ALTER TABLE tt1 ADD CONSTRAINT fk2 FOREIGN KEY (tt2ref) REFERENCES tt2(ID);

-- CREATE SIMPLE FUNCTIONS - INVALID

-- Exception no returns
/*e*/CREATE FUNCTION function_test()

-- Exception no body
/*e*/CREATE FUNCTION function_test() RETURNS INTEGER
  BEGIN ATOMIC
  END
/*e*/call function_test()
/*e*/DROP FUNCTION function_test

-- Exception - invalid in parameter
/*e*/CREATE FUNCTION function_test(IN value INTEGER) RETURNS INTEGER
  RETURNS 10

-- Exception - not supported OUT parameter
/*e*/CREATE FUNCTION function_test(OUT val INTEGER) RETURNS INTEGER
  RETURNS 10

-- CREATE SIMPLE FUNCTIONS

-- No body
CREATE FUNCTION function_test() RETURNS INTEGER
  RETURN 10;
/*r10*/call function_test()
DROP FUNCTION function_test

-- Simple function - IN parameter
CREATE FUNCTION function_test(IN val INTEGER) RETURNS INTEGER
  RETURN val + 10;
/*r20*/call function_test(10)
DROP FUNCTION function_test

-- Simple select
CREATE FUNCTION function_test() RETURNS INTEGER
  RETURN (SELECT id FROM tt2)
INSERT INTO tt2(ID) VALUES(1);
/*r1*/call function_test()
DROP FUNCTION function_test

CREATE TABLE CUSTOMER(ID INTEGER PRIMARY KEY,FIRSTNAME VARCHAR(255), LASTNAME VARCHAR(255),STREET VARCHAR(255),CITY VARCHAR(255));

INSERT INTO CUSTOMER VALUES(8,'Andrew','Miller','288 - 20th Ave.','Seattle');
INSERT INTO CUSTOMER VALUES(9,'James','Schneider','277 Seventh Av.','Berne');
INSERT INTO CUSTOMER VALUES(10,'Anne','Fuller','135 Upland Pl.','Dallas');
INSERT INTO CUSTOMER VALUES(11,'Julia','White','412 Upland Pl.','Chicago');
INSERT INTO CUSTOMER VALUES(12,'George','Ott','381 Upland Pl.','Palo Alto');

CREATE FUNCTION test( in x INT)
 returns table(i INT, v VARCHAR(100))
 begin atomic
   declare y int;
   set y = x;
   return TABLE(select id, firstname from customer where id > y);
 end

call test(10);
