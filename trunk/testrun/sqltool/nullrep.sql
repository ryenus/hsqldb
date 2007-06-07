/*
 * $Id$
 *
 * Tests enforcement of null-representation token 
 */

CREATE TABLE t (i INT, vc VARCHAR);

INSERT INTO t VALUES(1, 'one');
/** For INPUT, the NULLREP is only used for DSV imports, since unquoted
 *  null works perfectly for other forms of input.
 *  Therefore, following should enter "[null]" literally.
 */
INSERT INTO t VALUES(2, '[null]');
INSERT INTO t VALUES(3, null);

* COUNT _
SELECT count(*) FROM t WHERE vc IS NULL;

* if (*COUNT != 1)
    \q Seems that non-DSV insertion of '[null]' inserted a real NULL
* end if

* VAL _
SELECT vc FROM t WHERE vc = '[null]';
* if (*VAL != [null])
    \q Failed to SELECT literal '[null]'
* end if

SELECT vc FROM t WHERE vc IS null;
* VAL _
SELECT vc FROM t WHERE vc IS null;
* if (*VAL != [null])
    \p BAD
    \p *{VAL}
    \q Database represented by *{VAL} instead of "[null]" in VARCHAR SELECT
* end if
