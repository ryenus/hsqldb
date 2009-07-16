/* Copyright (c) 2001-2009, The HSQL Development Group
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


package org.hsqldb.persist;

import java.util.Enumeration;

import org.hsqldb.Database;
import org.hsqldb.DatabaseURL;
import org.hsqldb.Error;
import org.hsqldb.ErrorCode;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.Set;
import org.hsqldb.lib.SimpleLog;
import org.hsqldb.lib.StringUtil;

/**
 * Manages a .properties file for a database.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.0
 */
public class HsqlDatabaseProperties extends HsqlProperties {

    private static final String hsqldb_method_class_names =
        "hsqldb.method_class_names";
    private static HashSet accessibleJavaMethodNames;

    static {
        try {
            String prop = System.getProperty(hsqldb_method_class_names);

            if (prop != null) {
                accessibleJavaMethodNames = new HashSet();

                String[] names = StringUtil.split(prop, ";");

                for (int i = 0; i < names.length; i++) {
                    accessibleJavaMethodNames.add(names[i]);
                }
            }
        } catch (Exception e) {}
    }

    /**
     * If the system property "hsqldb.method_class_names" is not set, then
     * static methods of all available Java classes can be accessed as functions
     * in HSQLDB. If the property is set, then only the list of semicolon
     * seperated method names becomes accessible. An empty property value means
     * no class is accessible.<p>
     *
     * A property value that ends with .* is treated as a wild card and allows
     * access to all classe or method names formed by substitution of the
     * asterisk.<p>
     *
     * All methods of org.hsqldb.Library and java.lang.Math are always
     * accessible.
     *
     *
     */
    public static boolean supportsJavaMethod(String name) {

        if (accessibleJavaMethodNames == null) {
            return true;
        }

        if (name.startsWith("org.hsqldb.Library.")) {
            return true;
        }

        if (name.startsWith("java.lang.Math.")) {
            return true;
        }

        if (accessibleJavaMethodNames.contains(name)) {
            return true;
        }

        Iterator it = accessibleJavaMethodNames.iterator();

        while (it.hasNext()) {
            String className = (String) it.next();
            int    limit     = className.lastIndexOf(".*");

            if (limit < 1) {
                continue;
            }

            if (name.startsWith(className.substring(0, limit + 1))) {
                return true;
            }
        }

        return false;
    }

    // accessibility
    private static final int SYSTEM_PROPERTY = 0;
    private static final int FILE_PROPERTY   = 1;
    private static final int SQL_PROPERTY    = 2;

    // db files modified
    public static final int     FILES_NOT_MODIFIED = 0;
    public static final int     FILES_MODIFIED     = 1;
    public static final int     FILES_NEW          = 2;
    private static final String MODIFIED_NO        = "no";
    private static final String MODIFIED_YES       = "yes";
    private static final String MODIFIED_NEW       = "no-new-files";

    // allowed property metadata
    private static final HashMap dbMeta   = new HashMap(67);
    private static final HashMap textMeta = new HashMap(17);

    // versions
    public static final String VERSION_STRING_1_7_0 = "1.7.0";
    public static final String VERSION_STRING_1_8_0 = "1.8.0";
    public static final String VERSION_STRING_1_9_0 = "1.9.0";
    public static final String THIS_VERSION         = "1.9.0";
    public static final String THIS_FULL_VERSION    = "1.9.0.0";
    public static final String THIS_CACHE_VERSION   = "1.9.0";
    public static final String PRODUCT_NAME         = "HSQL Database Engine";
    public static final int    MAJOR                = 1,
                               MINOR                = 9,
                               REVISION             = 0;

    /**
     * system properties supported by HSQLDB
     */
    public static final String system_lockfile_poll_retries_property =
        "hsqldb.lockfile_poll_retries";
    public static final String system_max_char_or_varchar_display_size =
        "hsqldb.max_char_or_varchar_display_size";

    //
    public static final String hsqldb_inc_backup = "hsqldb.incremental_backup";

    //
    public static final String  hsqldb_version  = "version";
    public static final String  hsqldb_readonly = "readonly";
    private static final String hsqldb_modified = "modified";

    //
    public static final String runtime_gc_interval = "runtime.gc_interval";

    //
    public static final String url_ifexists       = "ifexists";
    public static final String url_default_schema = "default_schema";

    //
    public static final String hsqldb_applog = "hsqldb.applog";
    public static final String hsqldb_cache_file_scale =
        "hsqldb.cache_file_scale";
    public static final String hsqldb_cache_free_count_scale =
        "hsqldb.cache_free_count_scale";
    public static final String hsqldb_cache_rows = "hsqldb.cache_rows";
    public static final String hsqldb_cache_size = "hsqldb.cache_size";
    public static final String hsqldb_compatible_version =
        "hsqldb.compatible_version";
    public static final String hsqldb_default_table_type =
        "hsqldb.default_table_type";
    public static final String hsqldb_defrag_limit   = "hsqldb.defrag_limit";
    public static final String hsqldb_files_readonly = "hsqldb.files_readonly";
    public static final String hsqldb_lock_file      = "hsqldb.lock_file";
    public static final String hsqldb_log_size       = "hsqldb.log_size";
    public static final String hsqldb_nio_data_file  = "hsqldb.nio_data_file";
    public static final String hsqldb_max_nio_scale  = "hsqldb.max_nio_scale";
    public static final String hsqldb_script_format  = "hsqldb.script_format";
    public static final String hsqldb_temp_directory = "hsqldb.temp_directory";
    public static final String hsqldb_result_max_memory_rows =
        "hsqldb.result_max_memory_rows";
    public static final String hsqldb_write_delay = "hsqldb.write_delay";

    //
    public static final String sql_compare_in_locale = "sql.compare_in_locale";
    public static final String sql_enforce_size      = "sql.enforce_size";
    public static final String sql_enforce_strict_size =
        "sql.enforce_strict_size";
    public static final String sql_enforce_names = "sql.enforce_names";

    //
    public static final String textdb_cache_scale = "textdb.cache_scale";
    public static final String textdb_cache_size_scale =
        "textdb.cache_size_scale";
    public static final String textdb_all_quoted = "textdb.all_quoted";
    public static final String textdb_allow_full_path =
        "textdb.allow_full_path";
    public static final String textdb_encoding     = "textdb.encoding";
    public static final String textdb_ignore_first = "textdb.ignore_first";
    public static final String textdb_quoted       = "textdb.quoted";
    public static final String textdb_fs           = "textdb.fs";
    public static final String textdb_vs           = "textdb.vs";
    public static final String textdb_lvs          = "textdb.lvs";

    //
    public static final String url_storage_class_name = "storage_class_name";
    public static final String url_fileaccess_class_name =
        "fileaccess_class_name";
    public static final String url_storage_key = "storage_key";
    public static final String url_shutdown    = "shutdown";

    static {

        // text table defaults
        textMeta.put(textdb_allow_full_path,
                     HsqlProperties.getMeta(textdb_allow_full_path,
                                            SYSTEM_PROPERTY, false));
        textMeta.put(textdb_quoted,
                     HsqlProperties.getMeta(textdb_quoted, SQL_PROPERTY,
                                            false));
        textMeta.put(textdb_all_quoted,
                     HsqlProperties.getMeta(textdb_all_quoted, SQL_PROPERTY,
                                            false));
        textMeta.put(textdb_ignore_first,
                     HsqlProperties.getMeta(textdb_ignore_first, SQL_PROPERTY,
                                            false));
        textMeta.put(textdb_fs,
                     HsqlProperties.getMeta(textdb_fs, SQL_PROPERTY, ","));
        textMeta.put(textdb_vs,
                     HsqlProperties.getMeta(textdb_vs, SQL_PROPERTY, null));
        textMeta.put(textdb_lvs,
                     HsqlProperties.getMeta(textdb_lvs, SQL_PROPERTY, null));
        textMeta.put(textdb_encoding,
                     HsqlProperties.getMeta(textdb_encoding, SQL_PROPERTY,
                                            "ISO-8859-1"));
        textMeta.put(textdb_cache_scale,
                     HsqlProperties.getMeta(textdb_cache_scale, SQL_PROPERTY,
                                            10, 8, 16));
        textMeta.put(textdb_cache_size_scale,
                     HsqlProperties.getMeta(textdb_cache_size_scale,
                                            SQL_PROPERTY, 10, 6, 20));
        dbMeta.putAll(textMeta);

        // string defaults for protected props
        dbMeta.put(hsqldb_version,
                   HsqlProperties.getMeta(hsqldb_version, FILE_PROPERTY,
                                          null));
        dbMeta.put(hsqldb_compatible_version,
                   HsqlProperties.getMeta(hsqldb_compatible_version,
                                          FILE_PROPERTY, null));
        dbMeta.put(hsqldb_modified,
                   HsqlProperties.getMeta(hsqldb_modified, FILE_PROPERTY,
                                          null));

        // boolean defaults for protected props
        dbMeta.put(hsqldb_readonly,
                   HsqlProperties.getMeta(hsqldb_readonly, FILE_PROPERTY,
                                          false));
        dbMeta.put(hsqldb_files_readonly,
                   HsqlProperties.getMeta(hsqldb_files_readonly,
                                          FILE_PROPERTY, false));

        // integral defaults for protected props
        // this property is either 1 or 8 - only 8 for new databases
        dbMeta.put(hsqldb_cache_file_scale,
                   HsqlProperties.getMeta(hsqldb_cache_file_scale,
                                          FILE_PROPERTY, 8, new int[] {
            1, 8
        }));

        // string defaults for user defined props
        dbMeta.put(hsqldb_temp_directory,
                   HsqlProperties.getMeta(hsqldb_temp_directory, SQL_PROPERTY,
                                          null));
        dbMeta.put(hsqldb_default_table_type,
                   HsqlProperties.getMeta(hsqldb_default_table_type,
                                          SQL_PROPERTY, "MEMORY"));

        // boolean defaults for user defined props
        dbMeta.put(hsqldb_inc_backup,
                   HsqlProperties.getMeta(hsqldb_inc_backup, SQL_PROPERTY,
                                          true));
        dbMeta.put(hsqldb_lock_file,
                   HsqlProperties.getMeta(hsqldb_lock_file, SQL_PROPERTY,
                                          true));
        dbMeta.put(hsqldb_nio_data_file,
                   HsqlProperties.getMeta(hsqldb_nio_data_file, SQL_PROPERTY,
                                          true));

        // char padding to size and exception if data is too long
        dbMeta.put(sql_enforce_size,
                   HsqlProperties.getMeta(sql_enforce_size, SQL_PROPERTY,
                                          true));

        dbMeta.put(sql_enforce_strict_size,
                   HsqlProperties.getMeta(sql_enforce_strict_size, SQL_PROPERTY,
                                          true));
        // SQL reserved words not allowed as some identifiers
        dbMeta.put(sql_enforce_names,
                   HsqlProperties.getMeta(sql_enforce_names, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_compare_in_locale,
                   HsqlProperties.getMeta(sql_compare_in_locale, SQL_PROPERTY,
                                          false));
        dbMeta.put(hsqldb_write_delay,
                   HsqlProperties.getMeta(hsqldb_write_delay, SQL_PROPERTY,
                                          true));

        // integral defaults for user-defined set props
        dbMeta.put(hsqldb_applog,
                   HsqlProperties.getMeta(hsqldb_applog, SQL_PROPERTY, 0, 0,
                                          2));
        dbMeta.put(hsqldb_script_format,
                   HsqlProperties.getMeta(hsqldb_script_format, SQL_PROPERTY,
                                          0, new int[] {
            0, 1, 3
        }));

        // integral defaults for user defined range props
        dbMeta.put(hsqldb_log_size,
                   HsqlProperties.getMeta(hsqldb_log_size, SQL_PROPERTY, 50,
                                          0, 1000));
        dbMeta.put(hsqldb_defrag_limit,
                   HsqlProperties.getMeta(hsqldb_defrag_limit, SQL_PROPERTY,
                                          20, 0, 100));
        dbMeta.put(runtime_gc_interval,
                   HsqlProperties.getMeta(runtime_gc_interval, SQL_PROPERTY,
                                          0, 0, 1000000));
        dbMeta.put(hsqldb_cache_size,
                   HsqlProperties.getMeta(hsqldb_cache_size, SQL_PROPERTY,
                                          10000, 100, 1000000));
        dbMeta.put(hsqldb_cache_rows,
                   HsqlProperties.getMeta(hsqldb_cache_rows, SQL_PROPERTY,
                                          50000, 100, 1000000));
        dbMeta.put(hsqldb_cache_free_count_scale,
                   HsqlProperties.getMeta(hsqldb_cache_free_count_scale,
                                          SQL_PROPERTY, 9, 6, 12));
        dbMeta.put(hsqldb_result_max_memory_rows,
                   HsqlProperties.getMeta(hsqldb_result_max_memory_rows,
                                          SQL_PROPERTY, 0, 0, 1000000));
        dbMeta.put(hsqldb_max_nio_scale,
                   HsqlProperties.getMeta(hsqldb_max_nio_scale, SQL_PROPERTY,
                                          28, 24, 31));
    }

    private Database database;

    public HsqlDatabaseProperties(Database db) {

        super(dbMeta, db.getPath(), db.logger.getFileAccess(),
              db.isFilesInJar());

        database = db;

        setNewDatabaseProperties();
    }

    void setNewDatabaseProperties() {

        // version of a new database
        setProperty(hsqldb_version, THIS_VERSION);

        // the earliest version that can open this database
        // this is set to 1.9.0 when the db is written to
        setProperty(hsqldb_compatible_version, VERSION_STRING_1_9_0);
        /*
             garbage collection with gc_interval
             Setting this value can be useful when HSQLDB is used as an
             in-process part of an application. The minimum practical
             amount is probably "10000" and the maximum "1000000"

             In some versions of Java, such as 1.3.1_02 on windows,
             when the application runs out of memory it runs the gc AND
             requests more memory from the OS. Setting this property
             forces the DB to live inside its memory budget but the
             maximum amount of memory can still be set with the
             java -Xmx argument to provide the memory needed by other
             parts of the app to do graphics and networking.

             Of course there is a speed penalty for setting the value
             too low and doing garbage collection too often.

             This was introduced as a result of tests by Karl Meissner
             (meissnersd@users)
         */

        // garbage collect per Record or Cache Row objects created
        // the default, "0" means no garbage collection is forced by
        // hsqldb (the Java Runtime will do it's own garbage collection
        // in any case).
        setProperty(runtime_gc_interval, 0);

        // type of logging (0 : text , 1 : binary, 3 : compressed)
        setProperty(hsqldb_script_format, 0);
        setProperty(hsqldb_readonly, false);
        setProperty(hsqldb_modified, "no-new-files");

        // the property "version" is also set to the current version
        //
        // the following properties can be set by the user as defaults for
        // text tables. the default values are shown.
        // "textdb.fs", ","
        // "textdb.vs", ",";
        // "textdb.lvs", ","
        // "textdb.ignore_first", false
        // "textdb.quoted", true
        // "textdb.all_quoted", false
        // "textdb.encoding", "ASCII"
        // "textdb.cache_scale", 10  -- allowed range 8-16
        // "textdb.cache_size_scale", 10  -- allowed range 8-20
        //
        // OOo related code
        if (database.logger.isStoredFileAccess()) {
            setProperty(hsqldb_cache_rows, 25000);
            setProperty(hsqldb_cache_size, 6000);
            setProperty(hsqldb_log_size, 10);
            setProperty(sql_enforce_size, true);
            setProperty(hsqldb_nio_data_file, false);
        }

        // OOo end
    }

    /**
     * Creates file with defaults if it didn't exist.
     * Returns false if file already existed.
     */
    public boolean load() {

        boolean exists;

        if (!DatabaseURL.isFileBasedDatabaseType(database.getType())) {
            return true;
        }

        try {
            exists = super.load();
        } catch (Exception e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_LOAD_SAVE_PROPERTIES, new Object[] {
                fileName, e
            });
        }

        if (!exists) {
            return false;
        }

        filterLoadedProperties();

        String version = getStringProperty(hsqldb_compatible_version);

        // do not open if the database belongs to a later (future) version
        int check = version.substring(0, 5).compareTo(THIS_VERSION);

        if (check > 0) {
            throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
        }

        if (getIntegerProperty(hsqldb_script_format) != 0) {
            throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
        }

        if (!THIS_VERSION.equals(getStringProperty(hsqldb_version))) {
            if (!MODIFIED_NO.equals(getStringProperty(hsqldb_modified))) {
                throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
            }
        }

        version = getStringProperty(hsqldb_version);

        return true;
    }

    public void save() {

        if (!DatabaseURL.isFileBasedDatabaseType(database.getType())
                || database.isFilesReadOnly() || database.isFilesInJar()) {
            return;
        }

        try {
            HsqlProperties props = new HsqlProperties(dbMeta,
                database.getPath(), database.logger.getFileAccess(), false);

            props.setProperty(hsqldb_version, THIS_VERSION);
            props.setProperty(hsqldb_compatible_version, VERSION_STRING_1_9_0);
            props.setProperty(hsqldb_modified, getProperty(hsqldb_modified));
            props.save(fileName + ".properties" + ".new");
            fa.renameElement(fileName + ".properties" + ".new",
                             fileName + ".properties");
        } catch (Exception e) {
            database.logger.appLog.logContext(SimpleLog.LOG_ERROR, "failed");

            throw Error.error(ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_LOAD_SAVE_PROPERTIES, new Object[] {
                fileName, e
            });
        }
    }

    void filterLoadedProperties() {

        Enumeration en = stringProps.propertyNames();

        while (en.hasMoreElements()) {
            String  key    = (String) en.nextElement();
            boolean accept = dbMeta.containsKey(key);

            if (!accept) {
                stringProps.remove(key);
            }
        }
    }

    /**
     *  overload file database properties with any passed on URL line
     *  do not store password etc
     */
    public void setURLProperties(HsqlProperties p) {

        if (p == null) {
            return;
        }

        for (Enumeration e = p.propertyNames(); e.hasMoreElements(); ) {
            String   propertyName = (String) e.nextElement();
            Object[] row          = (Object[]) dbMeta.get(propertyName);

            if (row != null
                    && ((Integer) row[HsqlProperties.indexType]).intValue()
                       == SQL_PROPERTY) {
                setDatabaseProperty(propertyName, p.getProperty(propertyName));
            }
        }
    }

    public Set getUserDefinedPropertyData() {

        Set      set = new HashSet();
        Iterator it  = dbMeta.values().iterator();

        while (it.hasNext()) {
            Object[] row = (Object[]) it.next();

            if (((Integer) row[HsqlProperties.indexType]).intValue()
                    == SQL_PROPERTY) {
                set.add(row);
            }
        }

        return set;
    }

    public boolean isUserDefinedProperty(String key) {

        Object[] row = (Object[]) dbMeta.get(key);

        return row != null
               && ((Integer) row[HsqlProperties.indexType]).intValue()
                  == SQL_PROPERTY;
    }

    public boolean isBoolean(String key) {

        Object[] row = (Object[]) dbMeta.get(key);

        return row != null && row[HsqlProperties.indexClass].equals("boolean")
               && ((Integer) row[HsqlProperties.indexType]).intValue()
                  == SQL_PROPERTY;
    }

    public boolean isIntegral(String key) {

        Object[] row = (Object[]) dbMeta.get(key);

        return row != null && row[HsqlProperties.indexClass].equals("int")
               && ((Integer) row[HsqlProperties.indexType]).intValue()
                  == SQL_PROPERTY;
    }

    public boolean isString(String key) {

        Object[] row = (Object[]) dbMeta.get(key);

        return row != null
               && row[HsqlProperties.indexClass].equals("java.lang.String")
               && ((Integer) row[HsqlProperties.indexType]).intValue()
                  == SQL_PROPERTY;
    }

    public boolean setDatabaseProperty(String key, String value) {

        Object[] meta  = (Object[]) dbMeta.get(key);
        String   error = HsqlProperties.validateProperty(key, value, meta);

        if (error != null) {
            return false;
        }

        stringProps.put(key, value);

        return true;
    }

    public int getDefaultWriteDelay() {

        // OOo related code
        if (database.logger.isStoredFileAccess()) {
            return 2000;
        }

        // OOo end
        return 10000;
    }

//---------------------
// new properties to review / persist
    public final static int NO_MESSAGE = 1;

    public int getErrorLevel() {

        //      return 0;
        return NO_MESSAGE;
    }

    public boolean divisionByZero() {
        return false;
    }

//------------------------
    public void setDBModified(int mode) {

        String value = MODIFIED_NO;

        if (mode == FILES_MODIFIED) {
            value = MODIFIED_YES;
        } else if (mode == FILES_NEW) {
            value = MODIFIED_NEW;
        }

        stringProps.put(hsqldb_modified, value);
        save();
    }

    public int getDBModified() {

        String value = getStringProperty("modified");

        if (MODIFIED_YES.equals(value)) {
            return FILES_MODIFIED;
        } else if (MODIFIED_NEW.equals(value)) {
            return FILES_NEW;
        }

        return FILES_NOT_MODIFIED;
    }

//-----------------------
    public String getProperty(String key) {

        Object[] metaData = (Object[]) dbMeta.get(key);

        if (metaData == null) {
            throw Error.error(ErrorCode.X_42511);
        }

        return stringProps.getProperty(key);
    }

    public boolean isPropertyTrue(String key) {

        Boolean  value;
        Object[] metaData = (Object[]) dbMeta.get(key);

        if (metaData == null) {
            throw Error.error(ErrorCode.X_42511);
        }

        value = (Boolean) metaData[HsqlProperties.indexDefaultValue];

        String prop = stringProps.getProperty(key);
        boolean isSystem =
            ((Integer) metaData[HsqlProperties.indexType]).intValue()
            == SYSTEM_PROPERTY;

        if (prop == null && isSystem) {
            prop = System.getProperty(key);
        }

        if (prop != null) {
            value = Boolean.valueOf(prop);
        }

        return value.booleanValue();
    }

    public String getStringProperty(String key) {

        String   value;
        Object[] metaData = (Object[]) dbMeta.get(key);

        if (metaData == null) {
            throw Error.error(ErrorCode.X_42511);
        }

        value = (String) metaData[HsqlProperties.indexDefaultValue];

        String prop = stringProps.getProperty(key);

        if (prop != null) {
            value = prop;
        }

        return value;
    }

    public int getIntegerProperty(String key) {

        int      value;
        Object[] metaData = (Object[]) dbMeta.get(key);

        if (metaData == null) {
            throw Error.error(ErrorCode.X_42511);
        }

        value =
            ((Integer) metaData[HsqlProperties.indexDefaultValue]).intValue();

        String prop = stringProps.getProperty(key);

        if (prop != null) {
            try {
                value = Integer.parseInt(prop);
            } catch (NumberFormatException e) {}
        }

        return value;
    }
}
