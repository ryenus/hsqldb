/* Copyright (c) 2001-2022, The HSQL Development Group
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
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.types.Collation;

/**
 * Manages a .properties file for a database.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.1
 * @since 1.7.0
 */
public class HsqlDatabaseProperties extends HsqlProperties {

    public static final String hsqldb_method_class_names =
        "hsqldb.method_class_names";
    public static final String textdb_allow_full_path =
        "textdb.allow_full_path";
    public static final String hsqldb_reconfig_logging =
        "hsqldb.reconfig_logging";
    public static String methodClassNames;
    private static final HashSet accessibleJavaMethodNames = new HashSet();
    private static boolean allowFullPath;

    static {
        try {
            methodClassNames = System.getProperty(hsqldb_method_class_names);

            if (methodClassNames != null) {

                String[] names = StringUtil.split(methodClassNames, ";");

                for (int i = 0; i < names.length; i++) {
                    accessibleJavaMethodNames.add(names[i]);
                }
            }

            String prop = System.getProperty(textdb_allow_full_path);

            if (prop != null) {
                if (Boolean.valueOf(prop)) {
                    allowFullPath = true;
                }
            }
        } catch (Exception e) {}
    }

    /**
     * If the system property "hsqldb.method_class_names" is not set, then
     * static methods of available Java classes cannot be accessed as functions
     * in HSQLDB. If the property is set, then only the list of semicolon
     * separated method names becomes accessible. An empty property value means
     * no class is accessible.<p>
     *
     * A property value that ends with .* is treated as a wild card and allows
     * access to all classe or method names formed by substitution of the
     * asterisk.<p>
     *
     * For example, org.mypackage.* means all classes in the given package.<p>
     *
     * All methods of java.lang.Math are always accessible.
     */
    public static boolean supportsJavaMethod(String name) {

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
    public static final int SYSTEM_PROP = 0;
    public static final int FILES_PROP  = 1;
    public static final int DB_PROP     = 2;
    public static final int SQL_PROP    = 3;
    public static final int URL_PROP    = 4;

    // db files modified
    public static final int FILES_NOT_MODIFIED      = 0;
    public static final int FILES_MODIFIED          = 1;
    public static final int FILES_MODIFIED_NEW      = 2;
    public static final int FILES_MODIFIED_NEW_DATA = 3;
    public static final int FILES_NEW               = 4;

    //
    private static final String MODIFIED_NO           = "no";
    private static final String MODIFIED_YES          = "yes";
    private static final String MODIFIED_YES_NEW      = "yes-new-files";
    private static final String MODIFIED_YES_NEW_DATA = "yes-new-files-data";
    private static final String MODIFIED_NO_NEW       = "no-new-files";

    // allowed property metadata
    private static final HashMap<String, PropertyMeta> dbMeta = new HashMap(128);
    private static final HashMap<String, PropertyMeta> textMeta = new HashMap(16);
    private static final HashSet<String>               excludedMeta = new HashSet();
    private static final HashMap<String, PropertyMeta> urlUserConnMeta = new HashMap();

    // versions
    public static final String VERSION_STRING_1_8_0 = "1.8.0";
    public static final String PRODUCT_NAME         = "HSQL Database Engine";

    public static final String THIS_VERSION      = "2.7.1";
    public static final String THIS_FULL_VERSION = "2.7.1";
    public static final int    MAJOR             = 2,
                               MINOR             = 7,
                               REVISION          = 1;

    /**
     * system properties supported by HSQLDB
     */
    public static final String system_lockfile_poll_retries_property =
        "hsqldb.lockfile_poll_retries";
    public static final String system_max_char_or_varchar_display_size =
        "hsqldb.max_char_or_varchar_display_size";

    //
    public static final String hsqldb_inc_backup = "hsqldb.inc_backup";

    //
    public static final String  hsqldb_version  = "version";
    public static final String  hsqldb_readonly = "readonly";
    private static final String hsqldb_modified = "modified";

    //
    public static final String tx_timestamp = "tx_timestamp";

    //
    public static final String runtime_gc_interval = "runtime.gc_interval";

    //
    public static final String url_ifexists          = "ifexists";
    public static final String url_create            = "create";
    public static final String url_default_schema    = "default_schema";
    public static final String url_check_props       = "check_props";
    public static final String url_get_column_name   = "get_column_name";
    public static final String url_close_result      = "close_result";
    public static final String url_allow_empty_batch = "allow_empty_batch";
    public static final String url_memory_lobs       = "memory_lobs";

    //
    public static final String url_shutdown    = "shutdown";
    public static final String url_recover     = "recover";
    public static final String url_tls_wrapper = "tls_wrapper";

    //
    public static final String url_crypt_key      = "crypt_key";
    public static final String url_crypt_type     = "crypt_type";
    public static final String url_crypt_provider = "crypt_provider";
    public static final String url_crypt_iv       = "crypt_iv";
    public static final String url_crypt_lobs     = "crypt_lobs";

    //
    public static final String hsqldb_tx       = "hsqldb.tx";
    public static final String hsqldb_tx_level = "hsqldb.tx_level";
    public static final String hsqldb_tx_conflict_rollback =
        "hsqldb.tx_conflict_rollback";
    public static final String hsqldb_tx_interrupt_rollback =
        "hsqldb.tx_interrupt_rollback";
    public static final String hsqldb_applog         = "hsqldb.applog";
    public static final String hsqldb_extlog         = "hsqldb.extlog";
    public static final String hsqldb_sqllog         = "hsqldb.sqllog";
    public static final String hsqldb_lob_file_scale = "hsqldb.lob_file_scale";
    public static final String hsqldb_lob_file_compressed =
        "hsqldb.lob_compressed";
    public static final String hsqldb_cache_file_scale =
        "hsqldb.cache_file_scale";
    public static final String hsqldb_cache_free_count =
        "hsqldb.cache_free_count";
    public static final String hsqldb_cache_rows = "hsqldb.cache_rows";
    public static final String hsqldb_cache_size = "hsqldb.cache_size";
    public static final String hsqldb_default_table_type =
        "hsqldb.default_table_type";
    public static final String hsqldb_defrag_limit   = "hsqldb.defrag_limit";
    public static final String hsqldb_files_readonly = "files_readonly";
    public static final String hsqldb_lock_file      = "hsqldb.lock_file";
    public static final String hsqldb_log_data       = "hsqldb.log_data";
    public static final String hsqldb_log_size       = "hsqldb.log_size";
    public static final String hsqldb_nio_data_file  = "hsqldb.nio_data_file";
    public static final String hsqldb_nio_max_size   = "hsqldb.nio_max_size";
    public static final String hsqldb_script_format  = "hsqldb.script_format";
    public static final String hsqldb_temp_directory = "hsqldb.temp_directory";
    public static final String hsqldb_result_max_memory_rows =
        "hsqldb.result_max_memory_rows";
    public static final String hsqldb_write_delay = "hsqldb.write_delay";
    public static final String hsqldb_write_delay_millis =
        "hsqldb.write_delay_millis";
    public static final String hsqldb_full_log_replay =
        "hsqldb.full_log_replay";
    public static final String hsqldb_large_data  = "hsqldb.large_data";
    public static final String hsqldb_files_space = "hsqldb.files_space";
    public static final String hsqldb_digest      = "hsqldb.digest";

    //
    public static final String jdbc_translate_tti_types =
        "jdbc.translate_tti_types";

    //
    public static final String sql_restrict_exec       = "sql.restrict_exec";
    public static final String sql_ref_integrity       = "sql.ref_integrity";
    public static final String sql_compare_in_locale = "sql.compare_in_locale";
    public static final String sql_enforce_size        = "sql.enforce_size";
    public static final String sql_enforce_strict_size =
        "sql.enforce_strict_size";    // synonym for sql_enforce_size
    public static final String sql_enforce_refs      = "sql.enforce_refs";
    public static final String sql_enforce_names     = "sql.enforce_names";
    public static final String sql_regular_names     = "sql.regular_names";
    public static final String sql_enforce_types     = "sql.enforce_types";
    public static final String sql_enforce_tdcd = "sql.enforce_tdc_delete";
    public static final String sql_enforce_tdcu = "sql.enforce_tdc_update";
    public static final String sql_char_literal      = "sql.char_literal";
    public static final String sql_concat_nulls      = "sql.concat_nulls";
    public static final String sql_nulls_first       = "sql.nulls_first";
    public static final String sql_nulls_order       = "sql.nulls_order";
    public static final String sql_unique_nulls      = "sql.unique_nulls";
    public static final String sql_convert_trunc     = "sql.convert_trunc";
    public static final String sql_trunc_trailing    = "sql.truncate_trailing";
    public static final String sql_avg_scale         = "sql.avg_scale";
    public static final String sql_max_recursive     = "sql.max_recursive";
    public static final String sql_double_nan        = "sql.double_nan";
    public static final String sql_syntax_db2        = "sql.syntax_db2";
    public static final String sql_syntax_mss        = "sql.syntax_mss";
    public static final String sql_syntax_mys        = "sql.syntax_mys";
    public static final String sql_syntax_ora        = "sql.syntax_ora";
    public static final String sql_syntax_pgs        = "sql.syntax_pgs";
    public static final String sql_longvar_is_lob    = "sql.longvar_is_lob";
    public static final String sql_default_collation = "sql.default_collation";
    public static final String sql_pad_space         = "sql.pad_space";
    public static final String sql_ignore_case       = "sql.ignore_case";
    public static final String sql_live_object       = "sql.live_object";
    public static final String sql_sys_index_names   = "sql.sys_index_names";
    public static final String sql_lowercase_ident   = "sql.lowercase_ident";

    //
    public static final String textdb_cache_scale = "textdb.cache_scale";
    public static final String textdb_cache_size_scale =
        "textdb.cache_size_scale";
    public static final String textdb_cache_rows   = "textdb.cache_rows";
    public static final String textdb_cache_size   = "textdb.cache_size";
    public static final String textdb_all_quoted   = "textdb.all_quoted";
    public static final String textdb_encoding     = "textdb.encoding";
    public static final String textdb_ignore_first = "textdb.ignore_first";
    public static final String textdb_quoted       = "textdb.quoted";
    public static final String textdb_fs           = "textdb.fs";
    public static final String textdb_vs           = "textdb.vs";
    public static final String textdb_lvs          = "textdb.lvs";
    public static final String textdb_qc           = "textdb.qc";
    public static final String textdb_null_def     = "textdb.null_def";

    // obsolete properties from version 2.5.1
    public static final String hsqldb_min_reuse     = "hsqldb.min_reuse";
    public static final String hsqldb_cache_version = "hsqldb.cache_version";

    static {

        // properties that are not displayed to user, including obsolete props
        excludedMeta.add(hsqldb_min_reuse);
        excludedMeta.add(hsqldb_cache_version);
        excludedMeta.add(runtime_gc_interval);
        excludedMeta.add(hsqldb_inc_backup);
        excludedMeta.add(tx_timestamp);
        excludedMeta.add(hsqldb_version);
        excludedMeta.add(hsqldb_modified);
        excludedMeta.add(hsqldb_full_log_replay);
        excludedMeta.add(sql_compare_in_locale);

        // url properties which apply to a new connection to an open database
        urlUserConnMeta.put(url_default_schema,
                            newMeta(url_default_schema, URL_PROP, false));
        urlUserConnMeta.put(url_check_props,
                            newMeta(url_check_props, URL_PROP, false));
        urlUserConnMeta.put(url_get_column_name,
                            newMeta(url_get_column_name, URL_PROP, true));
        urlUserConnMeta.put(url_close_result,
                            newMeta(url_close_result, URL_PROP, false));
        urlUserConnMeta.put(url_allow_empty_batch,
                            newMeta(url_allow_empty_batch, URL_PROP, false));
        urlUserConnMeta.put(url_memory_lobs,
                            newMeta(url_memory_lobs, URL_PROP, false));

        // text table defaults
        textMeta.put(textdb_allow_full_path,
                     newMeta(textdb_allow_full_path, SYSTEM_PROP, true));
        textMeta.put(textdb_quoted, newMeta(textdb_quoted, SQL_PROP, true));
        textMeta.put(textdb_all_quoted,
                     newMeta(textdb_all_quoted, SQL_PROP, false));
        textMeta.put(textdb_ignore_first,
                     newMeta(textdb_ignore_first, SQL_PROP, false));
        textMeta.put(textdb_null_def,
                     newMeta(textdb_null_def, SQL_PROP, false));
        textMeta.put(textdb_fs, newMeta(textdb_fs, SQL_PROP, ","));
        textMeta.put(textdb_vs, newMeta(textdb_vs, SQL_PROP, null));
        textMeta.put(textdb_lvs, newMeta(textdb_lvs, SQL_PROP, null));
        textMeta.put(textdb_qc, newMeta(textdb_qc, SQL_PROP, "\""));
        textMeta.put(textdb_encoding,
                     newMeta(textdb_encoding, SQL_PROP, "ISO-8859-1"));
        textMeta.put(textdb_cache_scale,
                     newMeta(textdb_cache_scale, DB_PROP, 10, 8, 16));
        textMeta.put(textdb_cache_size_scale,
                     newMeta(textdb_cache_size_scale, DB_PROP, 10, 6, 20));
        textMeta.put(textdb_cache_rows,
                     newMeta(textdb_cache_rows, DB_PROP, 1000, 100, 1000000));
        textMeta.put(textdb_cache_size,
                     newMeta(textdb_cache_size, DB_PROP, 100, 10, 1000000));
        dbMeta.putAll(textMeta);

        // system props
        dbMeta.put(hsqldb_method_class_names,
                   newMeta(hsqldb_method_class_names, SYSTEM_PROP, ""));
        dbMeta.put(hsqldb_reconfig_logging,
                   newMeta(hsqldb_reconfig_logging, SYSTEM_PROP, false));

        // string defaults for protected props
        dbMeta.put(hsqldb_version, newMeta(hsqldb_version, FILES_PROP, null));
        dbMeta.put(hsqldb_modified,
                   newMeta(hsqldb_modified, FILES_PROP, null));
        dbMeta.put(hsqldb_cache_version,
                   newMeta(hsqldb_cache_version, FILES_PROP, null));

        // boolean defaults for protected props
        dbMeta.put(hsqldb_readonly,
                   newMeta(hsqldb_readonly, FILES_PROP, false));
        dbMeta.put(hsqldb_files_readonly,
                   newMeta(hsqldb_files_readonly, FILES_PROP, false));

        // string defaults for user defined props
        dbMeta.put(hsqldb_tx, newMeta(hsqldb_tx, SQL_PROP, "LOCKS"));
        dbMeta.put(hsqldb_tx_level,
                   newMeta(hsqldb_tx_level, SQL_PROP, "READ_COMMITTED"));
        dbMeta.put(hsqldb_temp_directory,
                   newMeta(hsqldb_temp_directory, DB_PROP, null));
        dbMeta.put(hsqldb_default_table_type,
                   newMeta(hsqldb_default_table_type, SQL_PROP, "MEMORY"));
        dbMeta.put(hsqldb_digest, newMeta(hsqldb_digest, DB_PROP, "MD5"));
        dbMeta.put(sql_live_object, newMeta(sql_live_object, DB_PROP, false));
        dbMeta.put(tx_timestamp, newMeta(tx_timestamp, DB_PROP, 0));

        // boolean defaults for user defined props
        dbMeta.put(hsqldb_tx_conflict_rollback,
                   newMeta(hsqldb_tx_conflict_rollback, SQL_PROP, true));
        dbMeta.put(hsqldb_tx_interrupt_rollback,
                   newMeta(hsqldb_tx_interrupt_rollback, SQL_PROP, false));
        dbMeta.put(jdbc_translate_tti_types,
                   newMeta(jdbc_translate_tti_types, SQL_PROP, true));
        dbMeta.put(hsqldb_inc_backup,
                   newMeta(hsqldb_inc_backup, DB_PROP, true));
        dbMeta.put(hsqldb_lock_file, newMeta(hsqldb_lock_file, DB_PROP, true));
        dbMeta.put(hsqldb_log_data, newMeta(hsqldb_log_data, DB_PROP, true));
        dbMeta.put(hsqldb_nio_data_file,
                   newMeta(hsqldb_nio_data_file, DB_PROP, true));
        dbMeta.put(hsqldb_full_log_replay,
                   newMeta(hsqldb_full_log_replay, DB_PROP, false));
        dbMeta.put(hsqldb_write_delay,
                   newMeta(hsqldb_write_delay, DB_PROP, true));
        dbMeta.put(hsqldb_large_data,
                   newMeta(hsqldb_large_data, DB_PROP, false));
        dbMeta.put(sql_ref_integrity,
                   newMeta(sql_ref_integrity, SQL_PROP, true));
        dbMeta.put(sql_restrict_exec,
                   newMeta(sql_restrict_exec, SQL_PROP, false));

        // SQL reserved words not allowed as some identifiers
        dbMeta.put(sql_enforce_names,
                   newMeta(sql_enforce_names, SQL_PROP, false));
        dbMeta.put(sql_regular_names,
                   newMeta(sql_regular_names, SQL_PROP, true));
        dbMeta.put(sql_enforce_refs,
                   newMeta(sql_enforce_refs, SQL_PROP, false));

        // char padding to size and exception if data is too long
        dbMeta.put(sql_enforce_size,
                   newMeta(sql_enforce_size, SQL_PROP, true));
        dbMeta.put(sql_enforce_types,
                   newMeta(sql_enforce_types, SQL_PROP, false));
        dbMeta.put(sql_enforce_tdcd,
                   newMeta(sql_enforce_tdcd, SQL_PROP, true));
        dbMeta.put(sql_enforce_tdcu,
                   newMeta(sql_enforce_tdcu, SQL_PROP, true));
        dbMeta.put(sql_char_literal,
                   newMeta(sql_char_literal, SQL_PROP, true));
        dbMeta.put(sql_concat_nulls,
                   newMeta(sql_concat_nulls, SQL_PROP, true));
        dbMeta.put(sql_nulls_first, newMeta(sql_nulls_first, SQL_PROP, true));
        dbMeta.put(sql_nulls_order, newMeta(sql_nulls_order, SQL_PROP, true));
        dbMeta.put(sql_unique_nulls,
                   newMeta(sql_unique_nulls, SQL_PROP, true));
        dbMeta.put(sql_convert_trunc,
                   newMeta(sql_convert_trunc, SQL_PROP, true));
        dbMeta.put(sql_trunc_trailing,
                   newMeta(sql_trunc_trailing, SQL_PROP, true));
        dbMeta.put(sql_avg_scale, newMeta(sql_avg_scale, SQL_PROP, 0, 0, 10));
        dbMeta.put(sql_max_recursive,
                   newMeta(sql_max_recursive, SQL_PROP, 256, 16,
                           1024 * 1024 * 1024));
        dbMeta.put(sql_double_nan, newMeta(sql_double_nan, SQL_PROP, true));
        dbMeta.put(sql_syntax_db2, newMeta(sql_syntax_db2, SQL_PROP, false));
        dbMeta.put(sql_syntax_mss, newMeta(sql_syntax_mss, SQL_PROP, false));
        dbMeta.put(sql_syntax_mys, newMeta(sql_syntax_mys, SQL_PROP, false));
        dbMeta.put(sql_syntax_ora, newMeta(sql_syntax_ora, SQL_PROP, false));
        dbMeta.put(sql_syntax_pgs, newMeta(sql_syntax_pgs, SQL_PROP, false));
        dbMeta.put(sql_compare_in_locale,
                   newMeta(sql_compare_in_locale, SQL_PROP, false));
        dbMeta.put(sql_longvar_is_lob,
                   newMeta(sql_longvar_is_lob, SQL_PROP, false));
        dbMeta.put(sql_default_collation,
                   newMeta(sql_default_collation, SQL_PROP,
                           Collation.defaultCollationName));
        dbMeta.put(sql_pad_space, newMeta(sql_pad_space, SQL_PROP, true));
        dbMeta.put(sql_ignore_case, newMeta(sql_ignore_case, SQL_PROP, false));
        dbMeta.put(sql_sys_index_names,
                   newMeta(sql_sys_index_names, SQL_PROP, true));
        dbMeta.put(sql_lowercase_ident,
                   newMeta(sql_lowercase_ident, SQL_PROP, false));
        dbMeta.put(hsqldb_files_space,
                   newMeta(hsqldb_files_space, DB_PROP, false));

        // integral defaults for user-defined props - sets
        dbMeta.put(hsqldb_write_delay_millis,
                   newMeta(hsqldb_write_delay_millis, DB_PROP, 500, 0, 10000));
        dbMeta.put(hsqldb_applog, newMeta(hsqldb_applog, DB_PROP, 0, 0, 4));
        dbMeta.put(hsqldb_extlog, newMeta(hsqldb_extlog, DB_PROP, 0, 0, 4));
        dbMeta.put(hsqldb_sqllog, newMeta(hsqldb_sqllog, DB_PROP, 0, 0, 4));
        dbMeta.put(hsqldb_script_format,
                   newMeta(hsqldb_script_format, DB_PROP, 0, new int[] {
            0, 1, 3
        }));
        dbMeta.put(hsqldb_lob_file_scale,
                   newMeta(hsqldb_lob_file_scale, DB_PROP, 32, new int[] {
            1, 2, 4, 8, 16, 32
        }));
        dbMeta.put(hsqldb_lob_file_compressed,
                   newMeta(hsqldb_lob_file_compressed, DB_PROP, false));

        // this property is normally 8 - or 1 for old databases from early versions
        dbMeta.put(hsqldb_cache_file_scale,
                   newMeta(hsqldb_cache_file_scale, DB_PROP, 32, new int[] {
            1, 8, 16, 32, 64, 128, 256, 512, 1024
        }));

        // integral defaults for user defined props - ranges
        dbMeta.put(hsqldb_log_size,
                   newMeta(hsqldb_log_size, DB_PROP, 50, 0, 4 * 1024));
        dbMeta.put(hsqldb_defrag_limit,
                   newMeta(hsqldb_defrag_limit, DB_PROP, 0, 0, 100));
        dbMeta.put(runtime_gc_interval,
                   newMeta(runtime_gc_interval, DB_PROP, 0, 0, 1000000));
        dbMeta.put(hsqldb_cache_size,
                   newMeta(hsqldb_cache_size, DB_PROP, 10000, 100,
                           4 * 1024 * 1024));
        dbMeta.put(hsqldb_cache_rows,
                   newMeta(hsqldb_cache_rows, DB_PROP, 50000, 100,
                           4 * 1024 * 1024));
        dbMeta.put(hsqldb_cache_free_count,
                   newMeta(hsqldb_cache_free_count, DB_PROP, 512, 0, 4096));
        dbMeta.put(hsqldb_result_max_memory_rows,
                   newMeta(hsqldb_result_max_memory_rows, DB_PROP, 0, 0,
                           4 * 1024 * 1024));
        dbMeta.put(hsqldb_nio_max_size,
                   newMeta(hsqldb_nio_max_size, DB_PROP, 256, 64, 262144));
        dbMeta.put(hsqldb_min_reuse,
                   newMeta(hsqldb_min_reuse, DB_PROP, 0, 0, 1024 * 1024));
    }

    private Database database;

    public HsqlDatabaseProperties(Database db) {

        super(db.getPath(), db.logger.getFileAccess(), db.isFilesInJar());

        database = db;

        setNewDatabaseProperties();
    }

    void setNewDatabaseProperties() {

        // version of a new database
        setProperty(hsqldb_version, THIS_VERSION);
        setProperty(hsqldb_modified, MODIFIED_NO_NEW);
    }

    /**
     * Creates file with defaults if it didn't exist.
     * Returns false if file already existed.
     */
    public boolean load() {

        boolean exists;

        if (!database.getType().isFileBased()) {
            return true;
        }

        try {
            exists = super.load();
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_LOAD_SAVE_PROPERTIES, new String[] {
                t.toString(), fileName
            });
        }

        if (!exists) {
            return false;
        }

        filterLoadedProperties();

        String version = getStringProperty(hsqldb_version);
        int    check = version.substring(0, 5).compareTo(VERSION_STRING_1_8_0);

        // do not open early version databases
        if (check < 0) {
            throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
        }

        // do not open databases of 1.8 versions if script format is not compatible
        if (check == 0) {
            if (getIntegerProperty(hsqldb_script_format) != 0) {
                throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
            }
        }

        check = version.substring(0, 2).compareTo(THIS_VERSION);

        // do not open if the database belongs to a later (future) version (3.x)
        if (check > 0) {
            throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
        }

        return true;
    }

    public void save() {

        if (!database.getType().isFileBased() || database.isFilesReadOnly()
                || database.isFilesInJar()) {
            return;
        }

        try {
            HsqlProperties props = new HsqlProperties(database.getPath(),
                database.logger.getFileAccess(), false);

            if (getIntegerProperty(hsqldb_script_format) == 3) {
                props.setProperty(hsqldb_script_format, 3);
            }

            props.setProperty(hsqldb_version, THIS_VERSION);
            props.setProperty(
                tx_timestamp,
                Long.toString(database.logger.getFilesTimestamp()));
            props.setProperty(hsqldb_modified, getProperty(hsqldb_modified));
            props.save(fileName + ".properties" + ".new");
            fa.renameElementOrCopy(fileName + ".properties" + ".new",
                                   fileName + ".properties", database.logger);
        } catch (Throwable t) {
            database.logger.logSevereEvent("save failed", t);

            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_LOAD_SAVE_PROPERTIES, new String[] {
                t.toString(), fileName
            });
        }
    }

    void filterLoadedProperties() {

        String val = stringProps.getProperty(sql_enforce_strict_size);

        if (val != null) {
            stringProps.setProperty(sql_enforce_size, val);
        }

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

        boolean strict = false;

        if (p == null) {
            return;
        }

        String val = p.getProperty(sql_enforce_strict_size);

        if (val != null) {
            p.setProperty(sql_enforce_size, val);
            p.removeProperty(sql_enforce_strict_size);
        }

        strict = p.isPropertyTrue(url_check_props, false);

        for (Enumeration e = p.propertyNames(); e.hasMoreElements(); ) {
            String       propertyName  = (String) e.nextElement();
            String       propertyValue = p.getProperty(propertyName);
            boolean      valid         = false;
            boolean      validVal      = false;
            String       error         = null;
            PropertyMeta meta          = dbMeta.get(propertyName);

            if (meta != null) {
                if (meta.propType == DB_PROP || meta.propType == SQL_PROP) {
                    valid = true;
                    error = validateProperty(propertyName, propertyValue, meta);
                    validVal = error == null;
                }
            }

            if (propertyName.startsWith("sql.")
                    || propertyName.startsWith("hsqldb.")
                    || propertyName.startsWith("textdb.")) {
                if (strict && !valid) {
                    throw Error.error(ErrorCode.X_42555, propertyName);
                }

                if (strict && !validVal) {
                    throw Error.error(ErrorCode.X_42556, error);
                }
            }
        }

        for (Enumeration e = p.propertyNames(); e.hasMoreElements(); ) {
            String       propertyName = (String) e.nextElement();
            PropertyMeta meta         = dbMeta.get(propertyName);

            if (meta != null) {
                if (meta.propType == DB_PROP || meta.propType == SQL_PROP) {
                    setDatabaseProperty(propertyName, p.getProperty(propertyName));
                }
            }
        }
    }

    public static Iterator<PropertyMeta> getUserDefinedProperties() {

        return new Iterator() {

            Iterator<PropertyMeta> it = dbMeta.values().iterator();
            PropertyMeta current;
            boolean      b = filterToNext();

            public boolean hasNext() {
                return current != null;
            }

            public Object next() {

                PropertyMeta value = current;

                filterToNext();

                return value;
            }

            public int nextInt() {
                return 0;
            }

            public long nextLong() {
                return 0L;
            }

            public void remove() {}

            private boolean filterToNext() {

                while (it.hasNext()) {
                    current = it.next();

                    if (!excludedMeta.contains(current.propName)) {
                        return true;
                    }
                }

                current = null;

                return false;
            }
        };
    }

    public boolean setDatabaseProperty(String key, String value) {

        PropertyMeta meta  = dbMeta.get(key);
        String       error = validateProperty(key, value, meta);

        if (error != null) {
            return false;
        }

        stringProps.put(key, value);

        return true;
    }

    public int getDefaultWriteDelay() {
        return 500;
    }

    public void setDBModified(int mode) {

        String value;

        switch (mode) {

            case FILES_NOT_MODIFIED :
                value = MODIFIED_NO;
                break;

            case FILES_MODIFIED :
                value = MODIFIED_YES;
                break;

            case FILES_MODIFIED_NEW :
                value = MODIFIED_YES_NEW;
                break;

            case FILES_MODIFIED_NEW_DATA :
                value = MODIFIED_YES_NEW_DATA;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "HsqlDatabaseProperties");
        }

        stringProps.put(hsqldb_modified, value);
        save();
    }

    public int getDBModified() {

        String value = getStringProperty(hsqldb_modified);

        if (MODIFIED_YES.equals(value)) {
            return FILES_MODIFIED;
        } else if (MODIFIED_YES_NEW.equals(value)) {
            return FILES_MODIFIED_NEW;
        } else if (MODIFIED_YES_NEW_DATA.equals(value)) {
            return FILES_MODIFIED_NEW_DATA;
        } else if (MODIFIED_NO_NEW.equals(value)) {
            return FILES_NEW;
        }

        return FILES_NOT_MODIFIED;
    }

//-----------------------
    public String getProperty(String key) {

        PropertyMeta meta = dbMeta.get(key);

        if (meta == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }

        return stringProps.getProperty(key);
    }

    public boolean isPropertyTrue(String key) {

        Boolean      value;
        PropertyMeta meta = dbMeta.get(key);

        if (meta == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }

        value = (Boolean) meta.propDefaultValue;

        String  prop     = null;
        boolean isSystem = meta.propType == SYSTEM_PROP;

        if (isSystem) {
            try {
                prop = System.getProperty(key);
            } catch (SecurityException e) {}
        } else {
            prop = stringProps.getProperty(key);
        }

        if (prop != null) {
            value = Boolean.valueOf(prop);
        }

        return value.booleanValue();
    }

    public static String getStringPropertyDefault(String key) {

        PropertyMeta meta = dbMeta.get(key);

        if (meta == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }

        return (String) meta.propDefaultValue;
    }

    public String getStringProperty(String key) {

        String       value;
        PropertyMeta meta = dbMeta.get(key);

        if (meta == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }

        value = (String) meta.propDefaultValue;

        String prop = stringProps.getProperty(key);

        if (prop != null) {
            value = prop;
        }

        return value;
    }

    public int getIntegerProperty(String key) {

        int          value;
        PropertyMeta meta = dbMeta.get(key);

        if (meta == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }

        value = ((Integer) meta.propDefaultValue).intValue();

        String prop = stringProps.getProperty(key);

        if (prop != null) {
            try {
                value = Integer.parseInt(prop);
            } catch (NumberFormatException e) {}
        }

        return value;
    }

    public static PropertyMeta getMeta(String key) {
        return dbMeta.get(key);
    }

    public static int getIntegerPropertyDefault(String key) {

        int          value;
        PropertyMeta meta = dbMeta.get(key);

        if (meta == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }

        value = ((Integer) meta.propDefaultValue).intValue();

        return value;
    }

    public static int getPropertyWithinRange(String name, int number) {

        PropertyMeta meta = dbMeta.get(name);

        if (meta == null) {
            return number;
        }

        if (meta.propClass.equals("Integer")) {
            if (meta.propIsRange) {
                int low  = meta.propRangeLow;
                int high = meta.propRangeHigh;

                if (number < low) {
                    return low;
                } else if (high < number) {
                    return high;
                }
            }

            if (meta.propValues != null) {
                int[] values = meta.propValues;

                if (ArrayUtil.find(values, number) == -1) {
                    return values[0];
                }
            }
        }

        return number;
    }

    public static boolean validateProperty(String name, int number) {

        PropertyMeta meta = dbMeta.get(name);

        if (meta == null) {
            return false;
        }

        if (meta.propClass.equals("Integer")) {
            if (meta.propIsRange) {
                int low  = meta.propRangeLow;
                int high = meta.propRangeHigh;

                if (number < low || high < number) {
                    return false;
                }
            }

            if (meta.propValues != null) {
                int[] values = meta.propValues;

                if (ArrayUtil.find(values, number) == -1) {
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    public static Iterator<PropertyMeta> getUrlUserConnectionProperties() {
        return urlUserConnMeta.values().iterator();
    }

    public String getClientPropertiesAsString() {

        if (isPropertyTrue(jdbc_translate_tti_types)) {
            StringBuilder sb = new StringBuilder(jdbc_translate_tti_types);

            sb.append('=').append(true);

            return sb.toString();
        }

        return "";
    }

    public boolean isVersion18() {

        String version =
            getProperty(HsqlDatabaseProperties.hsqldb_cache_version,
                        THIS_VERSION);

        return version.substring(0, 4).equals("1.7.");
    }
}
