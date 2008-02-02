/*
 * $Id$
 *
 * Tests comments.  This comment itself is a multi-line comment
 */

/* Just to have a work table */
CREATE TABLE t(i int);

  /* A multi-line
  comment  with
  leading + trailing white space. */  

/*Repeat with text right up to edges.
 *
 * Tests comments.  This comment itself is a multi-line comment*/

  /*Repeat with text right up to edges.
  comment  with
  leading + trailing white space. */

/* Following line contains spaces */
           

/* Simple hyphen-hyphen comments */
-- blah
  -- blah

/* Empty and white space comments: */
/**/
/*  */
  /**/  
  /*  */
/* The second of each of the following pairs have trailing white space.
--
--  
  --
  --  

/* Nesting different comments inside one another */
/* -- The traditional comment should still close. */
INSERT INTO t VALUES (9);
SELECT * FROM t;  -- SQL-trailing comment #1

DROP table t;
