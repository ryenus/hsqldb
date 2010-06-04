/* Copyright (c) 2001-2010, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb.server;

import java.util.Enumeration;

import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.persist.HsqlProperties;

/**
 * A subclass HsqlProperties with functionality needed for the HSQLDB Server
 * implementations.<p>
 *
 * A property object is checked once and all the errors are stored in
 * collections to be used
 *
 * Meta records specify accepted keys and policies for the expected values.<p>
 *
 * Policy for defaults: <ul>
 *  <li>If (non-null) default is specified for the Meta record, then
 *  behavior is obvious.
 *  <li>If pattern-type Meta record, then there is no default.
 *  A value is required for the property.
 *  <li>Otherwise null is specified for the Meta record and user must set
 *  a value.
 * </ul>
 *
 * If a range is specified in the Meta record, then the value is checked
 * against the range.<p>
 *
 * If a set of values specified in the Meta record, then the value is checked
 * against the set.<p>
 *
 * @version 1.9.0
 * @since 1.9.0
 */
public class ServerProperties extends HsqlProperties {

    // types of properties
    final static int SERVER_PROPERTY       = 0;
    final static int SERVER_MULTI_PROPERTY = 1;
    final static int SYSTEM_PROPERTY       = 2;

    // keys to properties
    final static String sc_key_address = "server.address";
    final static String sc_key_autorestart_server =
        "server.restart_on_shutdown";
    final static String sc_key_database         = "server.database.";
    final static String sc_key_dbname           = "server.dbname.";
    final static String sc_key_no_system_exit   = "server.no_system_exit";
    final static String sc_key_port             = "server.port";
    final static String sc_key_http_port        = "server.port";
    final static String sc_key_silent           = "server.silent";
    final static String sc_key_tls              = "server.tls";
    final static String sc_key_trace            = "server.trace";
    final static String sc_key_web_default_page = "server.default_page";
    final static String sc_key_web_root         = "server.root";
    final static String sc_key_max_connections  = "server.maxconnections";
    final static String sc_key_remote_open_db   = "server.remote_open";
    final static String sc_key_max_databases    = "server.maxdatabases";
    final static String sc_key_acl              = "server.acl";
    final static String sc_key_daemon           = "server.daemon";
    final static String sc_key_system           = "system.";

    // web server page defaults
    final static String sc_default_web_mime = "text/html";
    final static String sc_default_web_page = "index.html";
    final static String sc_default_web_root = ".";

    //
    final static HashMap        meta     = new HashMap();
    final static OrderedHashSet prefixes = new OrderedHashSet();

    //
    final int         protocol;
    protected boolean initialised = false;

    //
    IntKeyHashMap  idToAliasMap = new IntKeyHashMap();
    IntKeyHashMap  idToPathMap  = new IntKeyHashMap();
    HashMappedList databases    = new HashMappedList();

    //
    HsqlArrayList errorList = new HsqlArrayList();

    ServerProperties(int protocol) {
        this.protocol = protocol;
    }

    ServerProperties(int protocol, String path) {

        super(path);

        this.protocol = protocol;
    }

    /**
     * Validates according to Meta map, and sets System Properties for those
     * properties with names matching the requisite pattern.
     */
    void validate() {

        Enumeration en = stringProps.propertyNames();

        while (en.hasMoreElements()) {
            String   key      = (String) en.nextElement();
            Object[] metadata = (Object[]) meta.get(key);

            if (metadata == null) {
                metadata = getPrefixedMetadata(key);
            }

            if (metadata == null) {
                String error = "unsupported property: " + key;

                errorList.add(error);

                continue;
            }

            String error = null;

            if (((Integer) metadata[indexType]).intValue()
                    == SYSTEM_PROPERTY) {
                error = validateSystemProperty(key, metadata);
            } else if (((Integer) metadata[indexType]).intValue()
                       == SERVER_MULTI_PROPERTY) {
                error = validateMultiProperty(key, metadata);
            } else {
                String value = getProperty(key);

                if (value == null) {
                    if (metadata[indexDefaultValue] == null) {
                        error = "missing value for property: " + key;
                    } else {
                        setProperty(key,
                                    metadata[indexDefaultValue].toString());
                    }
                } else {
                    error = HsqlProperties.validateProperty(key, value,
                            metadata);
                }
            }

            if (error != null) {
                errorList.add(error);
            }
        }

        Iterator it = idToAliasMap.keySet().iterator();

        while (it.hasNext()) {
            int number = it.nextInt();

            if (!idToPathMap.containsKey(number)) {
                errorList.add("no path for database id: " + number);
            }
        }

        it = idToPathMap.keySet().iterator();

        while (it.hasNext()) {
            int number = it.nextInt();

            if (!idToAliasMap.containsKey(number)) {
                errorList.add("no alias for database id: " + number);
            }
        }

        initialised = true;
    }

    Object[] getPrefixedMetadata(String key) {

        for (int i = 0; i < prefixes.size(); i++) {
            String prefix = (String) prefixes.get(i);

            if (key.startsWith(prefix)) {
                return (Object[]) meta.get(prefix);
            }
        }

        return null;
    }

    /**
     * Checks an alias or database path. Duplicates are checked as duplicate
     * numbering may result from differnt strings (e.g. 02 and 2).
     */
    String validateMultiProperty(String key, Object[] meta) {

        int    dbNumber;
        String prefix = (String) meta[indexName];

        try {
            dbNumber = Integer.parseInt(key.substring(prefix.length()));
        } catch (NumberFormatException e1) {
            return ("maformed database enumerator: " + key);
        }

        if (meta[indexName].equals(sc_key_dbname)) {
            String alias = stringProps.getProperty(key).toLowerCase();

            if (databases.containsKey(alias)) {
                return "duplicate alias: " + alias;
            }

            Object existing = idToAliasMap.put(dbNumber, alias);

            if (existing != null) {
                return "duplicate database enumerator: " + key;
            }
        } else if (meta[indexName].equals(sc_key_database)) {
            String path     = stringProps.getProperty(key);
            Object existing = idToPathMap.put(dbNumber, path);

            if (existing != null) {
                return "duplicate database enumerator: " + key;
            }
        }

        return null;
    }

    /**
     * System properties are currently not checked, as different libraries in
     * the environment may need different names?
     */
    String validateSystemProperty(String key, Object[] meta) {

        String prefix      = (String) meta[indexName];
        String specificKey = key.substring(prefix.length());
        String value       = stringProps.getProperty(key);

        if (value == null) {
            return "value required for property: " + key;
        }

        System.setProperty(specificKey, value);

        return null;
    }

    static {

        // properties with variable suffixes
        meta.put(sc_key_database,
                 getMeta(sc_key_database, SERVER_MULTI_PROPERTY, null));
        meta.put(sc_key_dbname,
                 getMeta(sc_key_dbname, SERVER_MULTI_PROPERTY, null));
        meta.put(sc_key_system, getMeta(sc_key_system, SYSTEM_PROPERTY, null));

        // properties with fixed names
        meta.put(sc_key_silent,
                 getMeta(sc_key_silent, SERVER_PROPERTY, false));
        meta.put(sc_key_trace, getMeta(sc_key_trace, SERVER_PROPERTY, false));
        meta.put(sc_key_tls, getMeta(sc_key_tls, SERVER_PROPERTY, false));
        meta.put(sc_key_acl, getMeta(sc_key_acl, SERVER_PROPERTY, false));
        meta.put(sc_key_autorestart_server,
                 getMeta(sc_key_autorestart_server, SERVER_PROPERTY, false));
        meta.put(sc_key_remote_open_db,
                 getMeta(sc_key_remote_open_db, SERVER_PROPERTY, false));
        meta.put(sc_key_no_system_exit,
                 getMeta(sc_key_no_system_exit, SERVER_PROPERTY, false));
        meta.put(sc_key_daemon, getMeta(sc_key_daemon, SERVER_PROPERTY, false));

        //
        prefixes.add(sc_key_database);
        prefixes.add(sc_key_dbname);
        prefixes.add(sc_key_system);

        //
        meta.put(sc_key_address,
                 getMeta(sc_key_address, SERVER_PROPERTY, null));
        meta.put(sc_key_port, getMeta(sc_key_port, 0, 9001, 0, 65535));
        meta.put(sc_key_http_port, getMeta(sc_key_http_port, 0, 80, 0, 65535));
        meta.put(sc_key_max_connections,
                 getMeta(sc_key_max_connections, 0, 100, 1, 10000));
        meta.put(sc_key_max_databases,
                 getMeta(sc_key_max_databases, 0, 10, 1, 1000));
    }
}
