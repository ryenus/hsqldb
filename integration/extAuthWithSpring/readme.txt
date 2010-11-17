$Id$

This is the home directory of the extAuthWithSpring sample.

It uses Spring to declaratively configure external authentication with a
master HyperSQL catalog, or with an LDAP server.

You will need Ant and a Java JDK installed.  To get started, invoke

    ant -Dauthentication.mode=HsqldbSlave

from this directory to run a JDBC app backed by an application database, with
authentication to the application database through another embedded master
database.  Play with the Spring bean files in the resources subdirectory to
switch the application database or the masterdatabase, or anything else.

If you have an LDAP server, edit "resources/ldapbeans.xml" to set settings
according to your LDAP server then run ant with the authentication.mode set to
"LDAP".
To help determine and test the settings that will work with your LDAP server, I
recommend that you use the program org.hsqldb.auth.LdapAuthBean.  See the
HyperSQL API Spec for org.hsqldb.auth.LdapAuthBean and the sample properties
file for it at "sample/ldap-exerciser.properties" in your HyperSQL distribution.
