/*
 * $Id$
 *
 * Tests PL control flow.  if, foreach statements, etc.
 */

*if (*UNSET)
    \q Failed boolean test of an unset variable
*end if
*if (astring)
   * y = something
*end if
*if (*X)
    \q Failed boolean test of a simple string constant
*end if
*if (0)
    \q Failed boolean test of zero constant
*end if
*if (! x)
/* Note that there must be white space to separate the two tokens above. */
    \q Failed boolean test of a plain constant
*end if


/* Nested if tests */
* if (1)
    * L1 = true
    * if (2)
        * L2 = true
    * end if
    * L11 = true
* end if
*if (! *L1)
    \q Pre-nest failure
*end if
*if (! *L2)
    \q Inside-nest failure
*end if
*if (! *L11)
    \q Post-nest failure
*end if
* L1 =
* L2 =
* L11 =
* if (1)
    * L1 = true
    * if (2)
        * L2 = true
    * end if
    * L11 = true
* end if
*if (! *L1)
    \q Pre-nest failure
*end if
*if (! *L2)
    \q Inside-nest failure
*end if
*if (! *L11)
    \q Post-nest failure
*end if
/* Test deep nesting of IFs, including negatives. */
* if (1)
    * L1 = true
    * if (0)
        * N2 = true
    * end if
    * if (2)
        * L2 = true
        * if (3)
            * L3 = true
            * if (4)
                * L4 = true
                * if (0)
                    * N5 = true
                * end if
                * if (5)
                    * L5 = true
                    * if (0)
                        * N6 = true
                    * end if
                    * if (6)
                        * L6 = true
                        * if (0)
                            * N7 = true
                        * end if
                    * end if
                    * L51 = true
                * end if
            * end if
            * if (0)
                * N4 = true
            * end if
            * L31 = true
        * end if
        * if (0)
            * N3 = true
        * end if
    * end if
    * L11 = true
* end if
*if (! *L1)
    \q Pre-deep-nest failure 1
*end if
*if (! *L2)
    \q Inside-deep-nest failure 2
*end if
*if (! *L11)
    \q Post-deep-nest failure 11
*end if
*if (! *L3)
    \q Pre-deep-nest failure 3
*end if
*if (! *L4)
    \q Inside-deep-nest failure 4
*end if
*if (! *L31)
    \q Post-deep-nest failure 31
*end if
*if (! *L5)
    \q Pre-deep-nest failure 5
*end if
*if (! *L6)
    \q Inside-deep-nest failure 6
*end if
*if (! *L51)
    \q Post-deep-nest failure 51
*end if
*if (*N2)
    \q Negative deep-nest failure 2
*end if
*if (*N3)
    \q Negative deep-nest failure 3
*end if
*if (*N4)
    \q Negative deep-nest failure 4
*end if
*if (*N5)
    \q Negative deep-nest failure 5
*end if
*if (*N6)
    \q Negative deep-nest failure 6
*end if
*if (*N7)
    \q Negative deep-nest failure 7
*end if

/* Nested foreach tests */
/* Initialize Results  to I */
* R = I
*foreach L1 (A B C)
  *foreach L2 (1 2 3 4)
      *foreach L3 (a b)
	      * R = *{R}:*{L1}*{L2}*{L3}
	  *end foreach
  *end foreach
*end foreach
*if (*R != I:A1a:A1b:A2a:A2b:A3a:A3b:A4a:A4b:B1a:B1b:B2a:B2b:B3a:B3b:B4a:B4b:C1a:C1b:C2a:C2b:C3a:C3b:C4a:C4b)
    \q nested foreach result unexpected: *{R}
*end if
/* Initialize Results  to I */
* R = I
*foreach L1 (A B C)
  *if (*L1 != A)
	  *foreach L2 (1 2 3 4)
		  *if (*L2 != 3)
			  *foreach L3 (a b c)
				  *if (*L3 != b)
					  * R = *{R}:*{L1}*{L2}*{L3}
				  *end if
			  *end foreach
		  *end if
	  *end foreach
  *end if
*end foreach
*if (*R != I:B1a:B1c:B2a:B2c:B4a:B4c:C1a:C1c:C2a:C2c:C4a:C4c)
    \q nested conditional foreach result unexpected: *{R}
*end if

/* Test break and continue */
/* Initialize Results  to I */
* R = I
*foreach L1 (A B C)
  *foreach L2 (1 2 3 4)
      *foreach L3 (a b)
          *if (*L3 == a)
            *continue
          *end if
	      * R = *{R}:*{L1}*{L2}*{L3}
	  *end foreach
      *if (*L2 == 3)
        *break foreach
      *end if
  *end foreach
*end foreach
*if (*R != I:A1b:A2b:A3b:B1b:B2b:B3b:C1b:C2b:C3b)
    \q nested foreach result unexpected: *{R}
*end if

/* If something doesn't work right, could get into infinite loop below. */
* accum=L
*while (1)
   * accum = *{accum}P
   * subcum = M
   *while (*subcum < MQQ)
       * subcum = *{subcum}Q
   *end while
   * accum = *{accum}*{subcum}
   *if (*accum == LPMQQPMQQ)
       *break while
    *end if
*end while

*if (*accum != LPMQQPMQQ)
    \p Wrong value accumulated by nested while loops (*{accum})
*end if
