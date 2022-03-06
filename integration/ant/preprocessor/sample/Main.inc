//#if (jdbc_version >= 4)
// JDBC Version is GTE 4:
    //#include Jdbc4.inc
         // This is where the JDBC 4 stuff will go...
    //#endinclude
//#elif (jdbc_version >= 3)
// JDBC Version is GTE 3 and LT 4:
    //#include Jdbc3.inc
         // This is where the JDBC 3 stuff will go...
    //#endinclude
//#elif (jdbc_version >= 2)
// JDBC Version is GTE 2 and LT 3:
    //#include Jdbc2.inc
        // This is where the JDBC 2 stuff will go...
    //#endinclude
//#elif (jdbc_version >= 1)
// JDBC Version is GTE 1 and LT 2:
//#else
// JDBC Version is LT 1:
//#endif