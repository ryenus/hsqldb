module org.hsqldb.sqltool {
    requires transitive java.logging;
    requires transitive java.sql;

    exports org.hsqldb.cmdline;
    exports org.hsqldb.cmdline.libclone;
    exports org.hsqldb.cmdline.sqltool;
    exports org.hsqldb.cmdline.utilclone;
}
