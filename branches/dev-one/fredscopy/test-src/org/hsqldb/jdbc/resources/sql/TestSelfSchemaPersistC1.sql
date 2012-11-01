-- This script series tests ALT commands on schema objects, including
-- persistence.  ALTER USER involves no schemas.
-- Commands tested:  ALTER INDEX, ALTER SEQUENCE, ALTER TABLE

-- The blaine schema will exist if a previous script created it already.
/*s*/DROP SCHEMA blaine CASCADE;
/*u0*/CREATE SCHEMA blaine authorization dba;

--                                                         PREP
-- Create a zillion simple objects to play with
/*u0*/SET SCHEMA public;
DROP TABLE public.mt00 IF EXISTS;
DROP TABLE public.mt01 IF EXISTS;
DROP TABLE public.mt02 IF EXISTS;
DROP TABLE public.mt03 IF EXISTS;
DROP TABLE mt04 IF EXISTS;
DROP TABLE mt05 IF EXISTS;
DROP TABLE mt06 IF EXISTS;
DROP TABLE mt07 IF EXISTS;
DROP TABLE public.mt08 IF EXISTS;
DROP TABLE public.mt09 IF EXISTS;
DROP TABLE public.mt10 IF EXISTS;
DROP TABLE public.mt11 IF EXISTS;
DROP TABLE public.mt12 IF EXISTS;
DROP TABLE public.mt13 IF EXISTS;
DROP TABLE mt14 IF EXISTS;
DROP TABLE mt15 IF EXISTS;
DROP TABLE mt16 IF EXISTS;
DROP TABLE mt17 IF EXISTS;
DROP TABLE public.mt18 IF EXISTS;
DROP TABLE public.mt19 IF EXISTS;
DROP TABLE public.mt20 IF EXISTS;
DROP TABLE public.mt21 IF EXISTS;
DROP TABLE public.mt22 IF EXISTS;
DROP TABLE public.mt23 IF EXISTS;
DROP TABLE public.mt24 IF EXISTS;
DROP TABLE public.mt25 IF EXISTS;
DROP TABLE public.mt26 IF EXISTS;
DROP TABLE public.mt27 IF EXISTS;
DROP TABLE public.mt28 IF EXISTS;
DROP TABLE public.mt29 IF EXISTS;
DROP TABLE ct00 IF EXISTS;
DROP TABLE ct01 IF EXISTS;
DROP TABLE ct02 IF EXISTS;
DROP TABLE ct03 IF EXISTS;
DROP TABLE ct04 IF EXISTS;
DROP TABLE ct05 IF EXISTS;
DROP TABLE ct06 IF EXISTS;
DROP TABLE ct07 IF EXISTS;
DROP TABLE ct08 IF EXISTS;
DROP TABLE ct09 IF EXISTS;
DROP TABLE ct10 IF EXISTS;
DROP TABLE ct11 IF EXISTS;
DROP TABLE ct12 IF EXISTS;
DROP TABLE ct13 IF EXISTS;
DROP TABLE ct14 IF EXISTS;
DROP TABLE ct15 IF EXISTS;
DROP TABLE ct16 IF EXISTS;
DROP TABLE ct17 IF EXISTS;
DROP TABLE ct18 IF EXISTS;
DROP TABLE ct19 IF EXISTS;
DROP TABLE t101 IF EXISTS;
CREATE TABLE mt00 (i int);
CREATE TABLE mt01 (i int);
CREATE TABLE mt02 (i int);
/*u0*/SET SCHEMA blaine;
CREATE TABLE public.mt03 (i int);
CREATE TABLE public.mt04 (i int);
CREATE TABLE public.mt05 (i int);
CREATE TABLE public.mt06 (i int);
CREATE TABLE public.mt07 (i int);
/*u0*/SET SCHEMA public;
CREATE TABLE mt08 (i int);
CREATE TABLE mt09 (i int);
CREATE TABLE mt10 (i int);
CREATE TABLE mt11 (i int);
CREATE TABLE mt12 (i int);
/*u0*/SET SCHEMA blaine;
CREATE TABLE public.mt13 (i int);
CREATE TABLE public.mt14 (i int);
CREATE TABLE public.mt15 (i int);
CREATE TABLE public.mt16 (i int);
CREATE TABLE public.mt17 (i int);
/*u0*/SET SCHEMA public;
CREATE TABLE mt18 (i int);
CREATE TABLE mt19 (i int);
CREATE TABLE mt20 (i int);
CREATE TABLE mt21 (i int);
CREATE TABLE mt22 (i int);
CREATE TABLE mt23 (i int);
CREATE TABLE mt24 (i int);
CREATE TABLE mt25 (i int);
CREATE TABLE mt26 (i int);
CREATE TABLE mt27 (i int);
CREATE TABLE mt28 (i int);
CREATE TABLE mt29 (i int);
CREATE CACHED TABLE ct00 (i int);
CREATE CACHED TABLE ct01 (i int);
CREATE CACHED TABLE ct02 (i int);
/*u0*/SET SCHEMA blaine;
CREATE CACHED TABLE public.ct03 (i int);
CREATE CACHED TABLE public.ct04 (i int);
CREATE CACHED TABLE public.ct05 (i int);
CREATE CACHED TABLE public.ct06 (i int);
CREATE CACHED TABLE public.ct07 (i int);
/*u0*/SET SCHEMA public;
CREATE CACHED TABLE ct08 (i int);
CREATE CACHED TABLE ct09 (i int);
CREATE CACHED TABLE ct10 (i int);
CREATE CACHED TABLE ct11 (i int);
CREATE CACHED TABLE ct12 (i int);
/*u0*/SET SCHEMA blaine;
CREATE CACHED TABLE public.ct13 (i int);
CREATE CACHED TABLE public.ct14 (i int);
CREATE CACHED TABLE public.ct15 (i int);
CREATE CACHED TABLE public.ct16 (i int);
CREATE CACHED TABLE public.ct17 (i int);
/*u0*/SET SCHEMA public;
CREATE CACHED TABLE ct18 (i int);
CREATE CACHED TABLE ct19 (i int);
DROP INDEX mi00 IF EXISTS;
DROP INDEX mi01 IF EXISTS;
DROP INDEX mi02 IF EXISTS;
DROP INDEX mi03 IF EXISTS;
DROP INDEX mi04 IF EXISTS;
DROP INDEX mui05 IF EXISTS;
DROP INDEX mui06 IF EXISTS;
DROP INDEX mui07 IF EXISTS;
DROP INDEX mui08 IF EXISTS;
DROP INDEX mui09 IF EXISTS;
DROP INDEX ci00 IF EXISTS;
DROP INDEX ci01 IF EXISTS;
DROP INDEX ci02 IF EXISTS;
DROP INDEX ci03 IF EXISTS;
DROP INDEX ci04 IF EXISTS;
DROP INDEX cui05 IF EXISTS;
DROP INDEX cui06 IF EXISTS;
DROP INDEX cui07 IF EXISTS;
DROP INDEX cui08 IF EXISTS;
DROP INDEX cui09 IF EXISTS;
DROP INDEX i101 IF EXISTS;
CREATE INDEX mi00 ON mt00 (i);
CREATE INDEX mi01 ON mt01 (i);
CREATE INDEX mi02 ON mt02 (i);
CREATE INDEX mi03 ON mt03 (i);
CREATE INDEX mi04 ON mt04 (i);
/*u0*/SET SCHEMA blaine;
CREATE INDEX public.mui05 ON public.mt05 (i);
CREATE INDEX public.mui06 ON public.mt06 (i);
CREATE INDEX public.mui07 ON public.mt07 (i);
CREATE INDEX public.mui08 ON public.mt08 (i);
CREATE INDEX public.mui09 ON public.mt09 (i);
CREATE INDEX public.ci00 ON public.ct00 (i);
CREATE INDEX public.ci01 ON public.ct01 (i);
CREATE INDEX public.ci02 ON public.ct02 (i);
CREATE INDEX public.ci03 ON public.ct03 (i);
CREATE INDEX public.ci04 ON public.ct04 (i);
CREATE INDEX public.cui05 ON public.ct05 (i);
/*u0*/SET SCHEMA public;
CREATE INDEX cui06 ON ct06 (i);
CREATE INDEX cui07 ON ct07 (i);
CREATE INDEX cui08 ON ct08 (i);
CREATE INDEX cui09 ON ct09 (i);
CREATE SEQUENCE s00;
CREATE SEQUENCE s01;
CREATE SEQUENCE s02;
/*u0*/SET SCHEMA blaine;
CREATE SEQUENCE public.s03;
CREATE SEQUENCE public.s04;
CREATE SEQUENCE public.s05;
CREATE SEQUENCE public.s06;
CREATE SEQUENCE public.s07;
/*u0*/SET SCHEMA public;
CREATE SEQUENCE s08;
CREATE SEQUENCE s09;
CREATE SEQUENCE s10;
CREATE SEQUENCE s11;
/*u0*/SET SCHEMA blaine;
CREATE SEQUENCE public.s12;
CREATE SEQUENCE public.s13;
CREATE SEQUENCE public.s14;
CREATE SEQUENCE public.s15;
CREATE SEQUENCE public.s16;
/*u0*/SET SCHEMA public;
CREATE SEQUENCE s17;
CREATE SEQUENCE s18;
CREATE SEQUENCE s19;
-- blaine schema
/*u0*/ SET SCHEMA blaine;
DROP TABLE bmt00 IF EXISTS;
DROP TABLE bmt01 IF EXISTS;
DROP TABLE bmt02 IF EXISTS;
DROP TABLE bmt03 IF EXISTS;
DROP TABLE bmt04 IF EXISTS;
DROP TABLE bmt05 IF EXISTS;
DROP TABLE bmt06 IF EXISTS;
DROP TABLE bmt07 IF EXISTS;
DROP TABLE bmt08 IF EXISTS;
DROP TABLE bmt09 IF EXISTS;
DROP TABLE bmt10 IF EXISTS;
DROP TABLE bmt11 IF EXISTS;
DROP TABLE bmt12 IF EXISTS;
DROP TABLE bmt13 IF EXISTS;
DROP TABLE bmt14 IF EXISTS;
DROP TABLE bmt15 IF EXISTS;
DROP TABLE bmt16 IF EXISTS;
DROP TABLE bmt17 IF EXISTS;
DROP TABLE bmt18 IF EXISTS;
DROP TABLE bmt19 IF EXISTS;
DROP TABLE bmt20 IF EXISTS;
DROP TABLE bmt21 IF EXISTS;
DROP TABLE bmt22 IF EXISTS;
DROP TABLE bmt23 IF EXISTS;
DROP TABLE bmt24 IF EXISTS;
DROP TABLE bmt25 IF EXISTS;
DROP TABLE bmt26 IF EXISTS;
DROP TABLE bmt27 IF EXISTS;
DROP TABLE bmt28 IF EXISTS;
DROP TABLE bmt29 IF EXISTS;
DROP TABLE bct00 IF EXISTS;
DROP TABLE bct01 IF EXISTS;
DROP TABLE bct02 IF EXISTS;
DROP TABLE bct03 IF EXISTS;
DROP TABLE bct04 IF EXISTS;
DROP TABLE bct05 IF EXISTS;
DROP TABLE bct06 IF EXISTS;
DROP TABLE bct07 IF EXISTS;
DROP TABLE bct08 IF EXISTS;
DROP TABLE bct09 IF EXISTS;
DROP TABLE bct10 IF EXISTS;
DROP TABLE bct11 IF EXISTS;
DROP TABLE bct12 IF EXISTS;
DROP TABLE bct13 IF EXISTS;
DROP TABLE bct14 IF EXISTS;
DROP TABLE bct15 IF EXISTS;
DROP TABLE bct16 IF EXISTS;
DROP TABLE bct17 IF EXISTS;
DROP TABLE bct18 IF EXISTS;
DROP TABLE bct19 IF EXISTS;
DROP TABLE bt101 IF EXISTS;
CREATE TABLE bmt00 (i int);
CREATE TABLE bmt01 (i int);
CREATE TABLE bmt02 (i int);
/*u0*/SET SCHEMA public;
CREATE TABLE blaine.bmt03 (i int);
CREATE TABLE blaine.bmt04 (i int);
CREATE TABLE blaine.bmt05 (i int);
CREATE TABLE blaine.bmt06 (i int);
CREATE TABLE blaine.bmt07 (i int);
/*u0*/SET SCHEMA blaine;
CREATE TABLE bmt08 (i int);
CREATE TABLE bmt09 (i int);
CREATE TABLE bmt10 (i int);
CREATE TABLE bmt11 (i int);
CREATE TABLE bmt12 (i int);
/*u0*/SET SCHEMA public;
CREATE TABLE blaine.bmt13 (i int);
CREATE TABLE blaine.bmt14 (i int);
CREATE TABLE blaine.bmt15 (i int);
CREATE TABLE blaine.bmt16 (i int);
CREATE TABLE blaine.bmt17 (i int);
/*u0*/SET SCHEMA blaine;
CREATE TABLE bmt18 (i int);
CREATE TABLE bmt19 (i int);
CREATE TABLE bmt20 (i int);
CREATE TABLE bmt21 (i int);
CREATE TABLE bmt22 (i int);
CREATE TABLE bmt23 (i int);
CREATE TABLE bmt24 (i int);
CREATE TABLE bmt25 (i int);
CREATE TABLE bmt26 (i int);
CREATE TABLE bmt27 (i int);
CREATE TABLE bmt28 (i int);
CREATE TABLE bmt29 (i int);
CREATE CACHED TABLE bct00 (i int);
CREATE CACHED TABLE bct01 (i int);
CREATE CACHED TABLE bct02 (i int);
/*u0*/SET SCHEMA public;
CREATE CACHED TABLE blaine.bct03 (i int);
CREATE CACHED TABLE blaine.bct04 (i int);
CREATE CACHED TABLE blaine.bct05 (i int);
CREATE CACHED TABLE blaine.bct06 (i int);
CREATE CACHED TABLE blaine.bct07 (i int);
/*u0*/SET SCHEMA blaine;
CREATE CACHED TABLE bct08 (i int);
CREATE CACHED TABLE bct09 (i int);
CREATE CACHED TABLE bct10 (i int);
CREATE CACHED TABLE bct11 (i int);
CREATE CACHED TABLE bct12 (i int);
/*u0*/SET SCHEMA public;
CREATE CACHED TABLE blaine.bct13 (i int);
CREATE CACHED TABLE blaine.bct14 (i int);
CREATE CACHED TABLE blaine.bct15 (i int);
CREATE CACHED TABLE blaine.bct16 (i int);
CREATE CACHED TABLE blaine.bct17 (i int);
/*u0*/SET SCHEMA blaine;
CREATE CACHED TABLE bct18 (i int);
CREATE CACHED TABLE bct19 (i int);
DROP INDEX bmi00 IF EXISTS;
DROP INDEX bmi01 IF EXISTS;
DROP INDEX bmi02 IF EXISTS;
DROP INDEX bmi03 IF EXISTS;
DROP INDEX bmi04 IF EXISTS;
DROP INDEX bmui05 IF EXISTS;
DROP INDEX bmui06 IF EXISTS;
DROP INDEX bmui07 IF EXISTS;
DROP INDEX bmui08 IF EXISTS;
DROP INDEX bmui09 IF EXISTS;
DROP INDEX bci00 IF EXISTS;
DROP INDEX bci01 IF EXISTS;
DROP INDEX bci02 IF EXISTS;
DROP INDEX bci03 IF EXISTS;
DROP INDEX bci04 IF EXISTS;
DROP INDEX bcui05 IF EXISTS;
DROP INDEX bcui06 IF EXISTS;
DROP INDEX bcui07 IF EXISTS;
DROP INDEX bcui08 IF EXISTS;
DROP INDEX bcui09 IF EXISTS;
DROP INDEX bi101 IF EXISTS;
CREATE INDEX bmi00 ON bmt00 (i);
CREATE INDEX bmi01 ON bmt01 (i);
CREATE INDEX bmi02 ON bmt02 (i);
CREATE INDEX bmi03 ON bmt03 (i);
CREATE INDEX bmi04 ON bmt04 (i);
/*u0*/SET SCHEMA public;
CREATE INDEX blaine.bmui05 ON blaine.bmt05 (i);
CREATE INDEX blaine.bmui06 ON blaine.bmt06 (i);
CREATE INDEX blaine.bmui07 ON blaine.bmt07 (i);
CREATE INDEX blaine.bmui08 ON blaine.bmt08 (i);
CREATE INDEX blaine.bmui09 ON blaine.bmt09 (i);
/*u0*/SET SCHEMA blaine;
CREATE INDEX bci00 ON bct00 (i);
CREATE INDEX bci01 ON bct01 (i);
CREATE INDEX bci02 ON bct02 (i);
CREATE INDEX bci03 ON bct03 (i);
CREATE INDEX bci04 ON bct04 (i);
/*u0*/SET SCHEMA public;
CREATE INDEX blaine.bcui05 ON blaine.bct05 (i);
CREATE INDEX blaine.bcui06 ON blaine.bct06 (i);
CREATE INDEX blaine.bcui07 ON blaine.bct07 (i);
CREATE INDEX blaine.bcui08 ON blaine.bct08 (i);
CREATE INDEX blaine.bcui09 ON blaine.bct09 (i);
CREATE SEQUENCE blaine.bs00;
CREATE SEQUENCE blaine.bs01;
CREATE SEQUENCE blaine.bs02;
CREATE SEQUENCE blaine.bs03;
/*u0*/SET SCHEMA blaine;
CREATE SEQUENCE bs04;
CREATE SEQUENCE bs05;
CREATE SEQUENCE bs06;
CREATE SEQUENCE bs07;
CREATE SEQUENCE bs08;
/*u0*/SET SCHEMA public;
CREATE SEQUENCE blaine.bs09;
CREATE SEQUENCE blaine.bs10;
CREATE SEQUENCE blaine.bs11;
CREATE SEQUENCE blaine.bs12;
/*u0*/SET SCHEMA blaine;
CREATE SEQUENCE bs13;
CREATE SEQUENCE bs14;
CREATE SEQUENCE bs15;
CREATE SEQUENCE bs16;
CREATE SEQUENCE bs17;
/*u0*/SET SCHEMA public;
CREATE SEQUENCE blaine.bs18;
CREATE SEQUENCE blaine.bs19;

-- These are the only tests for SEQUENCEs in this script. SEQUENCES
-- The only ALTER command for sequences is here.
INSERT INTO public.mt01 VALUES(0);
INSERT INTO blaine.bmt01 VALUES(0);
/*u0*/SET SCHEMA blaine;
/*r0*/SELECT next value FOR public.s00 FROM public.mt01;
/*r1*/SELECT next value FOR public.s00 FROM public.mt01;
/*r0*/SELECT next value FOR public.s04 FROM blaine.bmt01;
/*r1*/SELECT next value FOR public.s04 FROM blaine.bmt01;
/*u0*/SET SCHEMA public;
/*r0*/SELECT next value FOR public.s01 FROM mt01;
/*r1*/SELECT next value FOR public.s01 FROM mt01;
/*r0*/SELECT next value FOR s02 FROM public.mt01;
/*r1*/SELECT next value FOR s02 FROM public.mt01;
/*r0*/SELECT next value FOR s03 FROM mt01;
/*r1*/SELECT next value FOR s03 FROM mt01;
/*r0*/SELECT next value FOR blaine.bs00 FROM blaine.bmt01;
-- Sequence inherits default schema from Session, not table.
/*e*/SELECT next value FOR bs00 FROM blaine.bmt01;
/*r0*/SELECT next value FOR blaine.bs04 FROM public.mt01;
/*r1*/SELECT next value FOR blaine.bs04 FROM public.mt01;
/*u0*/SET SCHEMA blaine;
/*r0*/SELECT next value FOR blaine.bs01 FROM bmt01;
/*r1*/SELECT next value FOR blaine.bs01 FROM bmt01;
/*r0*/SELECT next value FOR bs02 FROM blaine.bmt01;
/*r1*/SELECT next value FOR bs02 FROM blaine.bmt01;
/*r0*/SELECT next value FOR bs03 FROM bmt01;
/*r1*/SELECT next value FOR bs03 FROM bmt01;
/*u0*/ALTER SEQUENCE public.s00 RESTART WITH 21;
/*u0*/ALTER SEQUENCE bs00 RESTART WITH 22;
/*u0*/SET SCHEMA public;
/*u0*/ALTER SEQUENCE blaine.bs01 RESTART WITH 23;
/*u0*/ALTER SEQUENCE s01 RESTART WITH 24;
/*r21*/SELECT next value FOR public.s00 FROM public.mt01;
/*r22*/SELECT next value FOR blaine.bs00 FROM public.mt01;
/*r23*/SELECT next value FOR blaine.bs01 FROM public.mt01;
/*r24*/SELECT next value FOR s01 FROM public.mt01;

-- May only rename: Indexes, Tables, Columns              RENAMES
-- (Will only test Column renames if I have time)
-- Can't change schemas for existing objects.
-- 1st all permutations of PUBLICs -> blaines
/*e*/SELECT * FROM blaine.rbmt00;
/*e*/SELECT * FROM blaine.rbct00;
/*e*/SELECT * FROM public.rmt00;
/*e*/SELECT * FROM public.cmt00;
/*u0*/SET SCHEMA public;
/*e*/ALTER INDEX mi00 RENAME TO blaine.bi101;
/*e*/ALTER INDEX mui05 RENAME TO blaine.bi101;
/*e*/ALTER TABLE mt10 RENAME TO blaine.bt101;
/*e*/ALTER TABLE ct10 RENAME TO blaine.bt101;
/*e*/ALTER INDEX public.mi00 RENAME TO blaine.bi101;
/*e*/ALTER INDEX public.mui05 RENAME TO blaine.bi101;
/*e*/ALTER TABLE public.mt10 RENAME TO blaine.bt101;
/*e*/ALTER TABLE public.ct10 RENAME TO blaine.bt101;
/*e*/ALTER INDEX blaine.mi00 RENAME TO public.bi101;
/*e*/ALTER INDEX blaine.mui05 RENAME TO public.bi101;
/*u0*/SET SCHEMA blaine;
/*e*/ALTER TABLE mt10 RENAME TO public.bt101;
/*e*/ALTER TABLE ct10 RENAME TO public.bt101;
/*u0*/SET SCHEMA public;
/*u0*/ALTER TABLE blaine.bmt00 RENAME TO rbmt00;
/*u0*/ALTER TABLE blaine.bct00 RENAME TO blaine.rbct00;
/*u0*/ALTER INDEX blaine.bmi00 RENAME TO rbmi00;
/*u0*/SET SCHEMA blaine;
/*u0*/ALTER INDEX bci00 RENAME TO rbci00;
/*u0*/ALTER INDEX public.mi00 RENAME TO public.rmi00;
/*u0*/ALTER INDEX public.ci00 RENAME TO rci00;
/*u0*/SET SCHEMA public;
/*u0*/ALTER TABLE public.mt00 RENAME TO public.rmt00;
/*u0*/ALTER TABLE ct00 RENAME TO rct00;
/*u0*/SET SCHEMA public;
/*u0*/ALTER INDEX blaine.bmui05 RENAME TO rbmui05;
/*u0*/SET SCHEMA blaine;
/*u0*/ALTER INDEX bcui05 RENAME TO rbcui05;
/*u0*/ALTER INDEX public.mui05 RENAME TO public.rmui05;
/*u0*/ALTER INDEX public.cui05 RENAME TO rcui05;
/*u0*/SET SCHEMA public;
/*e*/ALTER INDEX public.mui05 RENAME TO blaine.bi101;
/*e*/ALTER TABLE public.mt10 RENAME TO blaine.bt101;
/*e*/ALTER TABLE public.ct10 RENAME TO blaine.bt101;
/*e*/ALTER INDEX public.mi00 RENAME TO blaine.bi101;
/*e*/ALTER INDEX public.mui05 RENAME TO blaine.bi101;
/*e*/ALTER TABLE public.mt10 RENAME TO blaine.bt101;
/*e*/ALTER TABLE public.ct10 RENAME TO blaine.bt101;
/*c0*/SELECT * FROM blaine.rbmt00;
/*c0*/SELECT * FROM blaine.rbct00;
/*c0*/SELECT * FROM public.rmt00;
/*c0*/SELECT * FROM public.rct00;

-- The only schema-specific ALTERs left are      ALTER TABLE ADD/DROP CONS
-- ADD NAMED Check/Unique CONSTRAINTS
-- First, CHECK constraints on MEM tables
/*e*/ALTER TABLE public.mt11 ADD CONSTRAINT blaine.mt11ck1 CHECK (i > 0);
/*e*/ALTER TABLE blaine.bmt11 ADD CONSTRAINT public.bmt11ck1 CHECK (i > 0);
/*e*/ALTER TABLE mt11 ADD CONSTRAINT blaine.mt11ck1 CHECK (i > 0);
/*u0*/ALTER TABLE mt11 ADD CONSTRAINT mt11ck1 CHECK (i > 0);
/*u0*/ALTER TABLE mt12 ADD CONSTRAINT public.mt12ck1 CHECK (i = 1);
SET SCHEMA blaine;
/*u0*/ALTER TABLE public.mt13 ADD CONSTRAINT mt13ck1 CHECK (i in (1, 2, 3));
/*u0*/ALTER TABLE public.mt14 ADD CONSTRAINT public.mt14ck1 CHECK (i != 0);
/*e*/ALTER TABLE blaine.bmt11 ADD CONSTRAINT public.bmt11ck1 CHECK (i > 0);
/*e*/ALTER TABLE public.mt11 ADD CONSTRAINT blaine.mt11ck1 CHECK (i > 0);
/*e*/ALTER TABLE bmt11 ADD CONSTRAINT public.bmt11ck1 CHECK (i > 0);
/*u0*/ALTER TABLE bmt11 ADD CONSTRAINT bmt11ck1 CHECK (i > 0);
/*u0*/ALTER TABLE bmt12 ADD CONSTRAINT blaine.bmt12ck1 CHECK (i = 1);
SET SCHEMA public;
/*u0*/ALTER TABLE blaine.bmt13 ADD CONSTRAINT bmt13ck1 CHECK (i in (1, 2, 3));
/*u0*/ALTER TABLE blaine.bmt14 ADD CONSTRAINT blaine.bmt14ck1 CHECK (i != 0);
/*e*/INSERT INTO public.mt11 values(0);
/*e*/INSERT INTO public.mt12 values(0);
/*e*/INSERT INTO public.mt13 values(0);
/*e*/INSERT INTO public.mt14 values(0);
/*e*/INSERT INTO blaine.bmt11 values(0);
/*e*/INSERT INTO blaine.bmt12 values(0);
/*e*/INSERT INTO blaine.bmt13 values(0);
/*e*/INSERT INTO blaine.bmt14 values(0);
/*u1*/INSERT INTO public.mt11 values(1);
/*u1*/INSERT INTO public.mt12 values(1);
/*u1*/INSERT INTO public.mt13 values(1);
/*u1*/INSERT INTO public.mt14 values(1);
/*u1*/INSERT INTO blaine.bmt11 values(1);
/*u1*/INSERT INTO blaine.bmt12 values(1);
/*u1*/INSERT INTO blaine.bmt13 values(1);
/*u1*/INSERT INTO blaine.bmt14 values(1);
-- Now, UNIQUE constraints on CACHED tables
/*e*/ALTER TABLE public.ct11 ADD CONSTRAINT blaine.ct11ck1 UNIQUE (i);
/*e*/ALTER TABLE blaine.bct11 ADD CONSTRAINT public.bct11ck1 UNIQUE (i);
/*e*/ALTER TABLE ct11 ADD CONSTRAINT blaine.ct11ck1 UNIQUE (i);
/*u0*/ALTER TABLE ct11 ADD CONSTRAINT ct11ck1 UNIQUE (i);
/*u0*/ALTER TABLE ct12 ADD CONSTRAINT public.ct12ck1 UNIQUE (i);
SET SCHEMA blaine;
/*u0*/ALTER TABLE public.ct13 ADD CONSTRAINT ct13ck1 UNIQUE (i);
/*u0*/ALTER TABLE public.ct14 ADD CONSTRAINT public.ct14ck1 UNIQUE (i);
/*e*/ALTER TABLE blaine.bct11 ADD CONSTRAINT public.bct11ck1 UNIQUE (i);
/*e*/ALTER TABLE public.ct11 ADD CONSTRAINT blaine.ct11ck1 UNIQUE (i);
/*e*/ALTER TABLE bct11 ADD CONSTRAINT public.bct11ck1 UNIQUE (i);
/*u0*/ALTER TABLE bct11 ADD CONSTRAINT bct11ck1 UNIQUE (i);
/*u0*/ALTER TABLE bct12 ADD CONSTRAINT blaine.bct12ck1 UNIQUE (i);
SET SCHEMA public;
/*u0*/ALTER TABLE blaine.bct13 ADD CONSTRAINT bct13ck1 UNIQUE (i);
/*u0*/ALTER TABLE blaine.bct14 ADD CONSTRAINT blaine.bct14ck1 UNIQUE (i);
/*u1*/INSERT INTO public.ct11 values(1);
/*u1*/INSERT INTO public.ct12 values(1);
/*u1*/INSERT INTO public.ct13 values(1);
/*u1*/INSERT INTO public.ct14 values(1);
/*u1*/INSERT INTO blaine.bct11 values(1);
/*u1*/INSERT INTO blaine.bct12 values(1);
/*u1*/INSERT INTO blaine.bct13 values(1);
/*u1*/INSERT INTO blaine.bct14 values(1);
/*e*/INSERT INTO public.ct11 values(1);
/*e*/INSERT INTO public.ct12 values(1);
/*e*/INSERT INTO public.ct13 values(1);
/*e*/INSERT INTO public.ct14 values(1);
/*e*/INSERT INTO blaine.bct11 values(1);
/*e*/INSERT INTO blaine.bct12 values(1);
/*e*/INSERT INTO blaine.bct13 values(1);
/*e*/INSERT INTO blaine.bct14 values(1);
-- ADD UNNAMED FK CONSTRAINTS
-- Index some MEM tables to reference. (table ct1[1-4],bct1[1-4] already set).
ALTER TABLE public.mt15 ADD unique(i);
ALTER TABLE public.mt16 ADD unique(i);
ALTER TABLE public.mt17 ADD unique(i);
ALTER TABLE public.mt18 ADD unique(i);
ALTER TABLE blaine.bmt15 ADD unique(i);
ALTER TABLE blaine.bmt16 ADD unique(i);
ALTER TABLE blaine.bmt17 ADD unique(i);
ALTER TABLE blaine.bmt18 ADD unique(i);
INSERT INTO public.mt15 VALUES(10);
INSERT INTO public.mt16 VALUES(10);
INSERT INTO public.mt17 VALUES(10);
INSERT INTO public.mt18 VALUES(10);
INSERT INTO blaine.bmt15 VALUES(10);
INSERT INTO blaine.bmt16 VALUES(10);
INSERT INTO blaine.bmt17 VALUES(10);
INSERT INTO blaine.bmt18 VALUES(10);
/*u0*/ALTER TABLE ct15 ADD FOREIGN KEY (i) REFERENCES mt15 (i);
/*u0*/ALTER TABLE ct16 ADD FOREIGN KEY (i) REFERENCES blaine.bct11 (i);
/*e*/ALTER TABLE public.ct16 ADD FOREIGN KEY (i) REFERENCES blaine.bct11 (i);
/*u0*/ALTER TABLE ct16 ADD FOREIGN KEY (i) REFERENCES public.mt16 (i);
SET SCHEMA blaine;
/*u0*/ALTER TABLE public.ct17 ADD FOREIGN KEY (i) REFERENCES public.mt17 (i);
/*e*/ALTER TABLE public.ct17 ADD FOREIGN KEY (i) REFERENCES public.mt17 (i);
/*u0*/ALTER TABLE public.ct18 ADD FOREIGN KEY (i) REFERENCES mt18 (i);
-- fks can't reference tables in other schemas.
/*u0*/ALTER TABLE bct15 ADD FOREIGN KEY (i) REFERENCES public.mt15 (i);
SET SCHEMA public;
/*u0*/ALTER TABLE blaine.bct17 ADD FOREIGN KEY (i) REFERENCES blaine.bmt17 (i);
/*u0*/ALTER TABLE blaine.bct18 ADD FOREIGN KEY (i) REFERENCES bmt18 (i);
/*u1*/INSERT INTO public.ct15 values(10);
/*e*/INSERT INTO public.ct16 values(10);
/*u1*/INSERT INTO public.ct17 values(10);
/*u1*/INSERT INTO blaine.bct18 values(10);
/*u1*/INSERT INTO blaine.bct17 values(10);
/*e*/INSERT INTO public.ct15 values(9);
/*e*/INSERT INTO public.ct16 values(9);
/*e*/INSERT INTO public.ct17 values(9);
/*e*/INSERT INTO public.bct18 values(9);
/*e*/INSERT INTO blaine.bct17 values(9);
-- Finally, ADD NAMED FK CONSTRAINTS
-- (TARGETS ct1[1-4],bct1[1-4] already set).
SET SCHEMA blaine;
/*e*/ALTER TABLE bmt21 ADD CONSTRAINT public.bmt21fk
    FOREIGN KEY (i) REFERENCES blaine.bct11 (i);
/*e*/ALTER TABLE bmt20 ADD CONSTRAINT public.bmt20fk
    FOREIGN KEY (i) REFERENCES bct12 (i);
/*u0*/ALTER TABLE bmt21 ADD CONSTRAINT bmt21fk
    FOREIGN KEY (i) REFERENCES blaine.bct11 (i);
/*e*/ALTER TABLE bmt21 ADD CONSTRAINT bmt21fk
    FOREIGN KEY (i) REFERENCES blaine.bct11 (i); -- Already exists
/*u0*/ALTER TABLE bmt20 ADD CONSTRAINT blaine.cmt20fk
    FOREIGN KEY (i) REFERENCES bct12 (i);
/*e*/ALTER TABLE bmt20 ADD CONSTRAINT blaine.cmt20fk
    FOREIGN KEY (i) REFERENCES bct12 (i); -- Already exists
SET SCHEMA public;
/*e*/ALTER TABLE blaine.bmt22 ADD CONSTRAINT public.bmt22fk
    FOREIGN KEY (i) REFERENCES blaine.bct13 (i);
/*e*/ALTER TABLE blaine.bmt23 ADD CONSTRAINT public.bmt23fk
    FOREIGN KEY (i) REFERENCES bct14 (i);
/*u0*/ALTER TABLE blaine.bmt22 ADD CONSTRAINT bmt2fk
    FOREIGN KEY (i) REFERENCES blaine.bct13 (i);
/*u0*/ALTER TABLE blaine.bmt23 ADD CONSTRAINT blaine.bmt23fk
    FOREIGN KEY (i) REFERENCES bct14 (i);
SET SCHEMA blaine;
/*e*/ALTER TABLE bmt24 ADD CONSTRAINT public.bmt24fk
    FOREIGN KEY (i) REFERENCES public.ct11 (i);
/*u0*/ALTER TABLE bmt24 ADD CONSTRAINT blaine.bmt24fk
    FOREIGN KEY (i) REFERENCES public.ct11 (i);
SET SCHEMA public;
/*e*/ALTER TABLE blaine.bmt22 ADD CONSTRAINT public.bmt22fk
    FOREIGN KEY (i) REFERENCES blaine.bct13 (i);
/*u0*/ALTER TABLE blaine.bmt25 ADD CONSTRAINT bmt25fk
    FOREIGN KEY (i) REFERENCES bct14 (i);
/*e*/INSERT INTO blaine.bmt21 VALUES (0);
/*u1*/INSERT INTO blaine.bmt21 VALUES (1);
/*e*/INSERT INTO blaine.bmt20 VALUES (0);
/*u1*/INSERT INTO blaine.bmt20 VALUES (1);
/*e*/INSERT INTO blaine.bmt22 VALUES (0);
/*u1*/INSERT INTO blaine.bmt22 VALUES (1);
/*e*/INSERT INTO blaine.bmt23 VALUES (0);
/*u1*/INSERT INTO blaine.bmt23 VALUES (1);
/*e*/INSERT INTO blaine.bmt25 VALUES (0);
/*u1*/INSERT INTO blaine.bmt25 VALUES (1);

SHUTDOWN IMMEDIATELY;