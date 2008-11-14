/*
 * $Id$
 *
 * Tests SQL/JRT
 */

create table customers(
    id INTEGER default 0, firstname VARCHAR, lastname VARCHAR,
    entrytime TIMESTAMP);

create procedure new_customer(firstname varchar(50), lastname varchar(50))
    insert into customers values (
        default, firstname, lastname, current_timestamp);
.;

SELECT count(*) FROM customers;
*if (*? != 0)
    \q SQL/PSM preparation failed
*end if

CALL new_customer('blaine', 'simpson');

SELECT count(*) FROM customers;
*if (*? != 1)
    \q SQL/PSM procedure failed
*end if
