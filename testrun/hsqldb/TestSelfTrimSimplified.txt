-- TestSelfTrimSimplified.txt
--
-- trim looks different


drop table testtable if exists;
create cached table testtable (
    aString              varchar(256)                   not null,
    firstNum             integer                        not null,
    aDate                date                           not null,
    secondNum            integer                        not null,
    thirdNum             integer                        not null,
    aName                varchar(32)                    not null
  );

insert into TESTTABLE(aString, firstNum, aDate, secondNum, thirdNum, aName)
  values (' XYZ ', 22, '2003-11-10', 18, 3, 'my name goes here');

select trim(aString) from testtable;
drop table testtable;
