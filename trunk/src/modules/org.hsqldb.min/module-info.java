module org.hsqldb {
   exports org.hsqldb.auth;
   exports org.hsqldb.jdbc;
   exports org.hsqldb.jdbc.pool;
   exports org.hsqldb.lib;
   exports org.hsqldb.lib.tar;
   exports org.hsqldb.trigger;

   requires java.logging;
   requires java.naming;
   requires java.sql;
   requires java.xml;

   provides java.sql.Driver with org.hsqldb.jdbc.JDBCDriver;
}
