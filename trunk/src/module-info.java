module org.hsqldb {
   exports org.hsqldb.auth;
   exports org.hsqldb.jdbc;
   exports org.hsqldb.jdbc.pool;
   exports org.hsqldb.lib;
   exports org.hsqldb.lib.tar;
   exports org.hsqldb.server;
   exports org.hsqldb.trigger;
   exports org.hsqldb.util;

   requires java.desktop;
   requires java.logging;
   requires java.naming;
   requires java.sql;
   requires java.xml;
   requires servlet.api;
}
