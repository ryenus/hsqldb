/*s*/DROP SCHEMA alts CASCADE;
CREATE SCHEMA alts AUTHORIZATION dba;
DROP VIEW v1 IF EXISTS;
DROP VIEW v2 IF EXISTS;
DROP VIEW v3 IF EXISTS;
DROP VIEW v4 IF EXISTS;
DROP VIEW v5 IF EXISTS;
DROP VIEW v6 IF EXISTS;
DROP VIEW v7 IF EXISTS;
DROP VIEW v8 IF EXISTS;
DROP TABLE t1 IF EXISTS;
DROP TABLE t2 IF EXISTS;
DROP TABLE t3 IF EXISTS;
DROP TABLE t4 IF EXISTS;
CREATE TABLE t1(t1c1 int, t1c2 int);
CREATE TABLE t2(t2c1 int, t2c2 int);
CREATE TABLE t3(c1 int, c2 int);
CREATE TABLE t4(c1 int, c2 int);
/*u1*/INSERT INTO t1 VALUES(1, 11);
/*u1*/INSERT INTO t2 VALUES(2, 12);
/*u1*/INSERT INTO t3 VALUES(3, 13);
/*u1*/INSERT INTO t4 VALUES(4, 14);

/*u0*/CREATE VIEW public.v1 AS SELECT * FROM t1;
/*u0*/CREATE VIEW public.v2 AS SELECT * FROM t2;
/*u0*/CREATE VIEW public.v3 AS SELECT * FROM t3;
/*u0*/CREATE VIEW public.v4 AS SELECT * FROM t4;
/*u0*/CREATE VIEW public.v5 (vc1, vc2) AS SELECT * FROM t1;
/*u0*/CREATE VIEW public.v6 (vc1, vc2) AS SELECT * FROM t2;
/*u0*/CREATE VIEW public.v7 (vc1, vc2) AS SELECT * FROM t3;
/*u0*/CREATE VIEW public.v8 (vc1, vc2) AS SELECT * FROM t4;

/*r1,11*/SELECT t1c1, t1c2 FROM v1;
/*r2,12*/SELECT t2c1, t2c2 FROM v2;
/*r3,13*/SELECT c1, c2 FROM v3;
/*r4,14*/SELECT c1, c2 FROM v4;
/*r1,11*/SELECT vc1, vc2 FROM v5;
/*r2,12*/SELECT vc1, vc2 FROM v6;
/*r3,13*/SELECT vc1, vc2 FROM v7;
/*r4,14*/SELECT vc1, vc2 FROM v8;

SET SCHEMA alts;
/*e*/SELECT t1c1, t1c2 FROM v1;
/*e*/SELECT t2c1, t2c2 FROM v2;
/*e*/SELECT c1, c2 FROM v3;
/*e*/SELECT c1, c2 FROM v4;
/*e*/SELECT vc1, vc2 FROM v5;
/*e*/SELECT vc1, vc2 FROM v6;
/*e*/SELECT vc1, vc2 FROM v7;
/*e*/SELECT vc1, vc2 FROM v8;
/*r1,11*/SELECT t1c1, t1c2 FROM public.v1;
/*r2,12*/SELECT t2c1, t2c2 FROM public.v2;
/*r3,13*/SELECT c1, c2 FROM public.v3;
/*r4,14*/SELECT c1, c2 FROM public.v4;
/*r1,11*/SELECT vc1, vc2 FROM public.v5;
/*r2,12*/SELECT vc1, vc2 FROM public.v6;
/*r3,13*/SELECT vc1, vc2 FROM public.v7;
/*r4,14*/SELECT vc1, vc2 FROM public.v8;
CREATE INDEX i1 ON public.t1(t1c1);
CREATE INDEX i2 ON public.t2(t2c1);
CREATE INDEX i3 ON public.t3(c1);
CREATE INDEX i4 ON public.t4(c1);
/*e*/SELECT t1c1, t1c2 FROM v1;
/*e*/SELECT t2c1, t2c2 FROM v2;
/*e*/SELECT c1, c2 FROM v3;
/*e*/SELECT c1, c2 FROM v4;
/*e*/SELECT vc1, vc2 FROM v5;
/*e*/SELECT vc1, vc2 FROM v6;
/*e*/SELECT vc1, vc2 FROM v7;
/*e*/SELECT vc1, vc2 FROM v8;
/*r1,11*/SELECT t1c1, t1c2 FROM public.v1;
/*r2,12*/SELECT t2c1, t2c2 FROM public.v2;
/*r3,13*/SELECT c1, c2 FROM public.v3;
/*r4,14*/SELECT c1, c2 FROM public.v4;
/*r1,11*/SELECT vc1, vc2 FROM public.v5;
/*r2,12*/SELECT vc1, vc2 FROM public.v6;
/*r3,13*/SELECT vc1, vc2 FROM public.v7;
/*r4,14*/SELECT vc1, vc2 FROM public.v8;

SET SCHEMA alts;
/*e*/SELECT t1c1, t1c2 FROM v1;
/*e*/SELECT t2c1, t2c2 FROM v2;
/*e*/SELECT c1, c2 FROM v3;
/*e*/SELECT c1, c2 FROM v4;
/*e*/SELECT vc1, vc2 FROM v5;
/*e*/SELECT vc1, vc2 FROM v6;
/*e*/SELECT vc1, vc2 FROM v7;
/*e*/SELECT vc1, vc2 FROM v8;
/*r1,11*/SELECT t1c1, t1c2 FROM public.v1;
/*r2,12*/SELECT t2c1, t2c2 FROM public.v2;
/*r3,13*/SELECT c1, c2 FROM public.v3;
/*r4,14*/SELECT c1, c2 FROM public.v4;
/*r1,11*/SELECT vc1, vc2 FROM public.v5;
/*r2,12*/SELECT vc1, vc2 FROM public.v6;
/*r3,13*/SELECT vc1, vc2 FROM public.v7;
/*r4,14*/SELECT vc1, vc2 FROM public.v8;