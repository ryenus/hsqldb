This directory contains access files for Apache Jackrabbit (http://jackrabbit.apache.org/)

The ddl file for jackrabbit version 2.x is located at:

resources/org/apache/jackrabbit/core/persistence/bundle/hsqldb.ddl

This ddl file can be used with the default BundleDbPersistenceManager

Copy the ddl file to the same directory in your Jackrabbit setup, alongside the existing 
ddl files. For example jackrabbit-standalone-2.2.4/org/apache/jackrabbit/core/persistence/bundle

A sample configuration is given below. The DDL table definitions use BLOBs, which 
are stored on disk.

If you are storing no more than several thousand objects, the non-blob fields can be stored in
memory for quicker access with hsqldb.default_table_type=memory. See the hsqldb documentation
at http://hsqldb.org/doc/2.0/ for different connection URL and other properties that can be used.

<PersistenceManager class="org.apache.jackrabbit.core.persistence.pool.BundleDbPersistenceManager">
<param name="driver" value="org.hsqldb.jdbcDriver"/>
<param name="url" value="jdbc:hsqldb:file:${wsp.home}/db;hsqldb.default_table_type=cached"/>
<param name="schemaObjectPrefix" value="${wsp.name}_"/>
<param name="databaseType" value="hsqldb"/>
</PersistenceManager>


A ddl file for older versions of Jackrabbit is also included in the ...core/persistence/db directory.

All files are modified copies of existing Jackrabbit sources.


