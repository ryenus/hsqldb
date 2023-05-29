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


package org.hsqldb;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.IntKeyIntValueHashMap;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.map.BitMap;
import org.hsqldb.map.ValuePool;
import org.hsqldb.persist.Crypto;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BinaryType;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.CharacterType;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.DTIType;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.IntervalMonthData;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.LobData;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;

/**
 * Implementation of HSQLDB functions that are not defined by the
 * SQL standard.<p>
 *
 * Some functions are translated into equivalent SQL Standard functions.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.0
 * @since 1.9.0
 */
public class FunctionCustom extends FunctionSQL {

    private static final int FUNC_ACOS                     = 71;
    private static final int FUNC_ACTION_ID                = 72;
    private static final int FUNC_ADD_MONTHS               = 73;
    private static final int FUNC_ASCII                    = 74;
    private static final int FUNC_ASCIISTR                 = 75;
    private static final int FUNC_ASIN                     = 76;
    private static final int FUNC_ATAN                     = 77;
    private static final int FUNC_ATAN2                    = 78;
    private static final int FUNC_BITAND                   = 79;
    private static final int FUNC_BITANDNOT                = 80;
    private static final int FUNC_BITNOT                   = 81;
    private static final int FUNC_BITOR                    = 82;
    private static final int FUNC_BITXOR                   = 83;
    private static final int FUNC_CHAR                     = 84;
    private static final int FUNC_CONCAT                   = 85;
    private static final int FUNC_COS                      = 86;
    private static final int FUNC_COSH                     = 87;
    private static final int FUNC_COT                      = 88;
    private static final int FUNC_CRYPT_KEY                = 89;
    private static final int FUNC_DATABASE                 = 90;
    private static final int FUNC_DATABASE_ISOLATION_LEVEL = 91;
    private static final int FUNC_DATABASE_NAME            = 92;
    private static final int FUNC_DATABASE_TIMEZONE        = 93;
    private static final int FUNC_DATABASE_VERSION         = 94;
    private static final int FUNC_DATE_ADD                 = 95;
    private static final int FUNC_DATE_SUB                 = 96;
    private static final int FUNC_DATEADD                  = 97;
    private static final int FUNC_DATEDIFF                 = 98;
    private static final int FUNC_DAYS                     = 99;
    private static final int FUNC_DBTIMEZONE               = 100;
    private static final int FUNC_DEGREES                  = 101;
    private static final int FUNC_DIAGNOSTICS              = 102;
    private static final int FUNC_DIFFERENCE               = 103;
    private static final int FUNC_FROM_BASE64              = 104;
    private static final int FUNC_FROM_TZ                  = 105;
    private static final int FUNC_HEX                      = 106;
    private static final int FUNC_HEXTORAW                 = 107;
    private static final int FUNC_IDENTITY                 = 108;
    private static final int FUNC_INSTR                    = 109;
    private static final int FUNC_ISAUTOCOMMIT             = 110;
    private static final int FUNC_ISOLATION_LEVEL          = 111;
    private static final int FUNC_ISREADONLYDATABASE       = 112;
    private static final int FUNC_ISREADONLYDATABASEFILES  = 113;
    private static final int FUNC_ISREADONLYSESSION        = 114;
    private static final int FUNC_LAST_DAY                 = 115;
    private static final int FUNC_LEFT                     = 116;
    private static final int FUNC_LOAD_FILE                = 117;
    private static final int FUNC_LOB_ID                   = 118;
    private static final int FUNC_LOCATE                   = 119;
    private static final int FUNC_LOG10                    = 120;
    private static final int FUNC_LPAD                     = 121;
    private static final int FUNC_LTRIM                    = 122;
    private static final int FUNC_MONTHS_BETWEEN           = 123;
    private static final int FUNC_NEW_TIME                 = 124;
    private static final int FUNC_NEXT_DAY                 = 125;
    private static final int FUNC_NUMTODSINTERVAL          = 126;
    private static final int FUNC_NUMTOYMINTERVAL          = 127;
    private static final int FUNC_PI                       = 128;
    private static final int FUNC_POSITION_ARRAY           = 129;
    private static final int FUNC_RADIANS                  = 130;
    private static final int FUNC_RAND                     = 131;
    private static final int FUNC_RAWTOHEX                 = 132;
    private static final int FUNC_REGEXP_COUNT             = 133;
    private static final int FUNC_REGEXP_INSTR             = 134;
    private static final int FUNC_REGEXP_LIKE              = 135;
    private static final int FUNC_REGEXP_MATCHES           = 136;
    private static final int FUNC_REGEXP_REPLACE           = 137;
    private static final int FUNC_REGEXP_SUBSTR            = 138;
    private static final int FUNC_REGEXP_SUBSTRING         = 139;
    private static final int FUNC_REGEXP_SUBSTRING_ARRAY   = 140;
    private static final int FUNC_REPEAT                   = 141;
    private static final int FUNC_REPLACE                  = 142;
    private static final int FUNC_REVERSE                  = 143;
    private static final int FUNC_RIGHT                    = 144;
    private static final int FUNC_ROUND                    = 145;
    private static final int FUNC_ROUNDMAGIC               = 146;
    private static final int FUNC_RPAD                     = 147;
    private static final int FUNC_RTRIM                    = 148;
    private static final int FUNC_SECONDS_MIDNIGHT         = 149;
    private static final int FUNC_SEQUENCE_ARRAY           = 150;
    private static final int FUNC_SESSION_ID               = 151;
    private static final int FUNC_SESSION_ISOLATION_LEVEL  = 152;
    private static final int FUNC_SESSION_TIMEZONE         = 153;
    private static final int FUNC_SESSIONTIMEZONE          = 154;
    private static final int FUNC_SIGN                     = 155;
    private static final int FUNC_SIN                      = 156;
    private static final int FUNC_SINH                     = 157;
    private static final int FUNC_SOUNDEX                  = 158;
    private static final int FUNC_SORT_ARRAY               = 159;
    private static final int FUNC_SPACE                    = 160;
    private static final int FUNC_SUBSTR                   = 161;
    private static final int FUNC_SYS_EXTRACT_UTC          = 162;
    private static final int FUNC_SYS_GUID                 = 163;
    private static final int FUNC_SYSDATE                  = 164;
    private static final int FUNC_SYSTIMESTAMP             = 165;
    private static final int FUNC_TAN                      = 166;
    private static final int FUNC_TANH                     = 167;
    private static final int FUNC_TIMESTAMP                = 168;
    private static final int FUNC_TIMESTAMP_WITH_ZONE      = 169;
    private static final int FUNC_TIMESTAMPADD             = 170;
    private static final int FUNC_TIMESTAMPDIFF            = 171;
    private static final int FUNC_TIMEZONE                 = 172;
    private static final int FUNC_TO_BASE64                = 173;
    private static final int FUNC_TO_CHAR                  = 174;
    private static final int FUNC_TO_DATE                  = 175;
    private static final int FUNC_TO_DSINTERVAL            = 176;
    private static final int FUNC_TO_YMINTERVAL            = 177;
    private static final int FUNC_TO_NUMBER                = 178;
    private static final int FUNC_TO_TIMESTAMP             = 179;
    private static final int FUNC_TO_TIMESTAMP_TZ          = 180;
    private static final int FUNC_TRANSACTION_CONTROL      = 181;
    private static final int FUNC_TRANSACTION_ID           = 182;
    private static final int FUNC_TRANSACTION_SIZE         = 183;
    private static final int FUNC_TRANSACTION_UTC          = 184;
    private static final int FUNC_TRANSLATE                = 185;
    private static final int FUNC_TRUNC                    = 186;
    private static final int FUNC_TRUNCATE                 = 187;
    private static final int FUNC_UNHEX                    = 188;
    private static final int FUNC_UNISTR                   = 189;
    private static final int FUNC_UUID                     = 190;
    private static final int FUNC_UNIX_TIMESTAMP           = 191;
    private static final int FUNC_UNIX_MILLIS              = 192;
    private static final int FUNC_DATEPART                 = 193;
    private static final int FUNC_DATENAME                 = 194;
    private static final int FUNC_NANVL                    = 195;
    private static final int FUNC_SQLCODE                  = 196;
    private static final int FUNC_SQLERRM                  = 197;

    //
    static final IntKeyIntValueHashMap customRegularFuncMap =
        new IntKeyIntValueHashMap();

    //J-
    static {
        nonDeterministicFuncSet.add(FUNC_ACTION_ID);
        nonDeterministicFuncSet.add(FUNC_CRYPT_KEY);
        nonDeterministicFuncSet.add(FUNC_DATABASE);
        nonDeterministicFuncSet.add(FUNC_DATABASE_ISOLATION_LEVEL);
        nonDeterministicFuncSet.add(FUNC_DATABASE_TIMEZONE);
        nonDeterministicFuncSet.add(FUNC_DBTIMEZONE);
        nonDeterministicFuncSet.add(FUNC_IDENTITY);
        nonDeterministicFuncSet.add(FUNC_ISAUTOCOMMIT);
        nonDeterministicFuncSet.add(FUNC_ISREADONLYSESSION);
        nonDeterministicFuncSet.add(FUNC_ISREADONLYDATABASE);
        nonDeterministicFuncSet.add(FUNC_ISREADONLYDATABASEFILES);
        nonDeterministicFuncSet.add(FUNC_ISOLATION_LEVEL);
        nonDeterministicFuncSet.add(FUNC_SESSION_ID);
        nonDeterministicFuncSet.add(FUNC_SESSION_ISOLATION_LEVEL);
        nonDeterministicFuncSet.add(FUNC_SESSION_TIMEZONE);
        nonDeterministicFuncSet.add(FUNC_SESSIONTIMEZONE);
        nonDeterministicFuncSet.add(FUNC_SYS_GUID);
        nonDeterministicFuncSet.add(FUNC_SYSDATE);
        nonDeterministicFuncSet.add(FUNC_SYSTIMESTAMP);
        nonDeterministicFuncSet.add(FUNC_TIMESTAMP);
        nonDeterministicFuncSet.add(FUNC_TIMEZONE);
        nonDeterministicFuncSet.add(FUNC_TRANSACTION_CONTROL);
        nonDeterministicFuncSet.add(FUNC_TRANSACTION_ID);
        nonDeterministicFuncSet.add(FUNC_TRANSACTION_SIZE);
        nonDeterministicFuncSet.add(FUNC_TRANSACTION_UTC);
        nonDeterministicFuncSet.add(FUNC_UUID);
        nonDeterministicFuncSet.add(FUNC_UNIX_TIMESTAMP);
        nonDeterministicFuncSet.add(FUNC_UNIX_MILLIS);

        //
        customRegularFuncMap.put(Tokens.ACOS, FUNC_ACOS);
        customRegularFuncMap.put(Tokens.ACTION_ID, FUNC_ACTION_ID);
        customRegularFuncMap.put(Tokens.ADD_MONTHS, FUNC_ADD_MONTHS);
        customRegularFuncMap.put(Tokens.ARRAY_SORT, FUNC_SORT_ARRAY);
        customRegularFuncMap.put(Tokens.ASCII, FUNC_ASCII);
        customRegularFuncMap.put(Tokens.ASCIISTR, FUNC_ASCIISTR);
        customRegularFuncMap.put(Tokens.ASIN, FUNC_ASIN);
        customRegularFuncMap.put(Tokens.ATAN, FUNC_ATAN);
        customRegularFuncMap.put(Tokens.ATAN2, FUNC_ATAN2);
        customRegularFuncMap.put(Tokens.BITAND, FUNC_BITAND);
        customRegularFuncMap.put(Tokens.BITANDNOT, FUNC_BITANDNOT);
        customRegularFuncMap.put(Tokens.BITLENGTH, FUNC_BIT_LENGTH);
        customRegularFuncMap.put(Tokens.BITNOT, FUNC_BITNOT);
        customRegularFuncMap.put(Tokens.BITOR, FUNC_BITOR);
        customRegularFuncMap.put(Tokens.BITXOR, FUNC_BITXOR);
        customRegularFuncMap.put(Tokens.CHAR, FUNC_CHAR);
        customRegularFuncMap.put(Tokens.CHR, FUNC_CHAR);
        customRegularFuncMap.put(Tokens.CONCAT_WORD, FUNC_CONCAT);
        customRegularFuncMap.put(Tokens.COS, FUNC_COS);
        customRegularFuncMap.put(Tokens.COSH, FUNC_COSH);
        customRegularFuncMap.put(Tokens.COT, FUNC_COT);
        customRegularFuncMap.put(Tokens.CRYPT_KEY, FUNC_CRYPT_KEY);
        customRegularFuncMap.put(Tokens.CURDATE, FUNC_CURRENT_DATE);
        customRegularFuncMap.put(Tokens.CURTIME, FUNC_LOCALTIME);
        customRegularFuncMap.put(Tokens.DATABASE, FUNC_DATABASE);
        customRegularFuncMap.put(Tokens.DATABASE_NAME, FUNC_DATABASE_NAME);
        customRegularFuncMap.put(Tokens.DATABASE_ISOLATION_LEVEL, FUNC_DATABASE_ISOLATION_LEVEL);
        customRegularFuncMap.put(Tokens.DATABASE_TIMEZONE, FUNC_DATABASE_TIMEZONE);
        customRegularFuncMap.put(Tokens.DATABASE_VERSION, FUNC_DATABASE_VERSION);
        customRegularFuncMap.put(Tokens.DATE_ADD, FUNC_DATE_ADD);
        customRegularFuncMap.put(Tokens.DATE_SUB, FUNC_DATE_SUB);
        customRegularFuncMap.put(Tokens.DATEADD, FUNC_DATEADD);
        customRegularFuncMap.put(Tokens.DATEDIFF, FUNC_DATEDIFF);
        customRegularFuncMap.put(Tokens.DATENAME, FUNC_DATENAME);
        customRegularFuncMap.put(Tokens.DATEPART, FUNC_DATEPART);
        customRegularFuncMap.put(Tokens.DAY, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.DAYNAME, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.DAYOFMONTH, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.DAYOFWEEK, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.DAYOFYEAR, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.DAYS, FUNC_DAYS);
        customRegularFuncMap.put(Tokens.DBTIMEZONE, FUNC_DBTIMEZONE);
        customRegularFuncMap.put(Tokens.DEGREES, FUNC_DEGREES);
        customRegularFuncMap.put(Tokens.DIAGNOSTICS, FUNC_DIAGNOSTICS);
        customRegularFuncMap.put(Tokens.DIFFERENCE, FUNC_DIFFERENCE);
        customRegularFuncMap.put(Tokens.EOMONTH, FUNC_LAST_DAY);
        customRegularFuncMap.put(Tokens.FROM_BASE64, FUNC_FROM_BASE64);
        customRegularFuncMap.put(Tokens.FROM_TZ, FUNC_FROM_TZ);
        customRegularFuncMap.put(Tokens.HEX, FUNC_HEX);
        customRegularFuncMap.put(Tokens.HEXTORAW, FUNC_HEXTORAW);
        customRegularFuncMap.put(Tokens.HOUR, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.IDENTITY, FUNC_IDENTITY);
        customRegularFuncMap.put(Tokens.INSERT, FUNC_OVERLAY_CHAR);
        customRegularFuncMap.put(Tokens.INSTR, FUNC_POSITION_CHAR);
        customRegularFuncMap.put(Tokens.IS_AUTOCOMMIT, FUNC_ISAUTOCOMMIT);
        customRegularFuncMap.put(Tokens.IS_READONLY_DATABASE, FUNC_ISREADONLYDATABASE);
        customRegularFuncMap.put(Tokens.IS_READONLY_DATABASE_FILES, FUNC_ISREADONLYDATABASEFILES);
        customRegularFuncMap.put(Tokens.IS_READONLY_SESSION, FUNC_ISREADONLYSESSION);
        customRegularFuncMap.put(Tokens.ISOLATION_LEVEL, FUNC_ISOLATION_LEVEL);
        customRegularFuncMap.put(Tokens.LAST_DAY, FUNC_LAST_DAY);
        customRegularFuncMap.put(Tokens.LCASE, FUNC_FOLD_LOWER);
        customRegularFuncMap.put(Tokens.LEFT, FUNC_LEFT);
        customRegularFuncMap.put(Tokens.LENGTH, FUNC_CHAR_LENGTH);
        customRegularFuncMap.put(Tokens.LOAD_FILE, FUNC_LOAD_FILE);
        customRegularFuncMap.put(Tokens.LOB_ID, FUNC_LOB_ID);
        customRegularFuncMap.put(Tokens.LOCATE, FUNC_POSITION_CHAR);
        customRegularFuncMap.put(Tokens.LOG, FUNC_LN);
        customRegularFuncMap.put(Tokens.LOG10, FUNC_LOG10);
        customRegularFuncMap.put(Tokens.LPAD, FUNC_LPAD);
        customRegularFuncMap.put(Tokens.LTRIM, FUNC_TRIM_CHAR);
        customRegularFuncMap.put(Tokens.MINUTE, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.MONTH, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.MONTHNAME, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.MONTHS_BETWEEN, FUNC_MONTHS_BETWEEN);
        customRegularFuncMap.put(Tokens.NANVL, FUNC_NANVL);
        customRegularFuncMap.put(Tokens.NEWID, FUNC_UUID);
        customRegularFuncMap.put(Tokens.NEXT_DAY, FUNC_NEXT_DAY);
        customRegularFuncMap.put(Tokens.NUMTODSINTERVAL, FUNC_NUMTODSINTERVAL);
        customRegularFuncMap.put(Tokens.NUMTOYMINTERVAL, FUNC_NUMTOYMINTERVAL);
        customRegularFuncMap.put(Tokens.OCTETLENGTH, FUNC_OCTET_LENGTH);
        customRegularFuncMap.put(Tokens.PI, FUNC_PI);
        customRegularFuncMap.put(Tokens.POSITION_ARRAY, FUNC_POSITION_ARRAY);
        customRegularFuncMap.put(Tokens.QUARTER, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.RADIANS, FUNC_RADIANS);
        customRegularFuncMap.put(Tokens.RAND, FUNC_RAND);
        customRegularFuncMap.put(Tokens.RAWTOHEX, FUNC_RAWTOHEX);
        customRegularFuncMap.put(Tokens.REGEXP_COUNT, FUNC_REGEXP_COUNT);
        customRegularFuncMap.put(Tokens.REGEXP_INSTR, FUNC_REGEXP_INSTR);
        customRegularFuncMap.put(Tokens.REGEXP_LIKE, FUNC_REGEXP_LIKE);
        customRegularFuncMap.put(Tokens.REGEXP_MATCHES, FUNC_REGEXP_MATCHES);
        customRegularFuncMap.put(Tokens.REGEXP_REPLACE, FUNC_REGEXP_REPLACE);
        customRegularFuncMap.put(Tokens.REGEXP_SUBSTR, FUNC_REGEXP_SUBSTR);
        customRegularFuncMap.put(Tokens.REGEXP_SUBSTRING, FUNC_REGEXP_SUBSTRING);
        customRegularFuncMap.put(Tokens.REGEXP_SUBSTRING_ARRAY, FUNC_REGEXP_SUBSTRING_ARRAY);
        customRegularFuncMap.put(Tokens.REPEAT, FUNC_REPEAT);
        customRegularFuncMap.put(Tokens.REPLACE, FUNC_REPLACE);
        customRegularFuncMap.put(Tokens.REVERSE, FUNC_REVERSE);
        customRegularFuncMap.put(Tokens.RIGHT, FUNC_RIGHT);
        customRegularFuncMap.put(Tokens.ROUND, FUNC_ROUND);
        customRegularFuncMap.put(Tokens.ROUNDMAGIC, FUNC_ROUNDMAGIC);
        customRegularFuncMap.put(Tokens.RPAD, FUNC_RPAD);
        customRegularFuncMap.put(Tokens.RTRIM, FUNC_TRIM_CHAR);
        customRegularFuncMap.put(Tokens.SECOND, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.SECONDS_MIDNIGHT, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.SEQUENCE_ARRAY, FUNC_SEQUENCE_ARRAY);
        customRegularFuncMap.put(Tokens.SESSION_ID, FUNC_SESSION_ID);
        customRegularFuncMap.put(Tokens.SESSION_ISOLATION_LEVEL, FUNC_SESSION_ISOLATION_LEVEL);
        customRegularFuncMap.put(Tokens.SESSION_TIMEZONE, FUNC_SESSION_TIMEZONE);
        customRegularFuncMap.put(Tokens.SESSIONTIMEZONE, FUNC_SESSIONTIMEZONE);
        customRegularFuncMap.put(Tokens.SIGN, FUNC_SIGN);
        customRegularFuncMap.put(Tokens.SIN, FUNC_SIN);
        customRegularFuncMap.put(Tokens.SINH, FUNC_SINH);
        customRegularFuncMap.put(Tokens.SORT_ARRAY, FUNC_SORT_ARRAY);
        customRegularFuncMap.put(Tokens.SOUNDEX, FUNC_SOUNDEX);
        customRegularFuncMap.put(Tokens.SPACE, FUNC_SPACE);
        customRegularFuncMap.put(Tokens.SUBSTR, FUNC_SUBSTRING_CHAR);
        customRegularFuncMap.put(Tokens.SYS_EXTRACT_UTC, FUNC_SYS_EXTRACT_UTC);
        customRegularFuncMap.put(Tokens.SYSDATE, FUNC_SYSDATE);
        customRegularFuncMap.put(Tokens.SYSTIMESTAMP, FUNC_SYSTIMESTAMP);
        customRegularFuncMap.put(Tokens.TAN, FUNC_TAN);
        customRegularFuncMap.put(Tokens.TANH, FUNC_TANH);
        customRegularFuncMap.put(Tokens.TIMESTAMP, FUNC_TIMESTAMP);
        customRegularFuncMap.put(Tokens.TIMESTAMP_WITH_ZONE, FUNC_TIMESTAMP_WITH_ZONE);
        customRegularFuncMap.put(Tokens.TIMESTAMPADD, FUNC_TIMESTAMPADD);
        customRegularFuncMap.put(Tokens.TIMESTAMPDIFF, FUNC_TIMESTAMPDIFF);
        customRegularFuncMap.put(Tokens.TIMEZONE, FUNC_TIMEZONE);
        customRegularFuncMap.put(Tokens.TO_BASE64, FUNC_TO_BASE64);
        customRegularFuncMap.put(Tokens.TO_CHAR, FUNC_TO_CHAR);
        customRegularFuncMap.put(Tokens.TO_DATE, FUNC_TO_DATE);
        customRegularFuncMap.put(Tokens.TO_DSINTERVAL, FUNC_TO_DSINTERVAL);
        customRegularFuncMap.put(Tokens.TO_YMINTERVAL, FUNC_TO_YMINTERVAL);
        customRegularFuncMap.put(Tokens.TO_NUMBER, FUNC_TO_NUMBER);
        customRegularFuncMap.put(Tokens.TO_TIMESTAMP, FUNC_TO_TIMESTAMP);
        customRegularFuncMap.put(Tokens.TO_TIMESTAMP_TZ, FUNC_TO_TIMESTAMP_TZ);
        customRegularFuncMap.put(Tokens.TRANSACTION_CONTROL, FUNC_TRANSACTION_CONTROL);
        customRegularFuncMap.put(Tokens.TRANSACTION_ID, FUNC_TRANSACTION_ID);
        customRegularFuncMap.put(Tokens.TRANSACTION_SIZE, FUNC_TRANSACTION_SIZE);
        customRegularFuncMap.put(Tokens.TRANSACTION_UTC, FUNC_TRANSACTION_UTC);
        customRegularFuncMap.put(Tokens.TRANSLATE, FUNC_TRANSLATE);
        customRegularFuncMap.put(Tokens.TRUNC, FUNC_TRUNC);
        customRegularFuncMap.put(Tokens.TRUNCATE, FUNC_TRUNCATE);
        customRegularFuncMap.put(Tokens.UCASE, FUNC_FOLD_UPPER);
        customRegularFuncMap.put(Tokens.UNIX_MILLIS, FUNC_UNIX_MILLIS);
        customRegularFuncMap.put(Tokens.UNIX_TIMESTAMP, FUNC_UNIX_TIMESTAMP);
        customRegularFuncMap.put(Tokens.SYS_GUID, FUNC_SYS_GUID);
        customRegularFuncMap.put(Tokens.UNHEX, FUNC_UNHEX);
        customRegularFuncMap.put(Tokens.UNISTR, FUNC_UNISTR);
        customRegularFuncMap.put(Tokens.UUID, FUNC_UUID);
        customRegularFuncMap.put(Tokens.WEEK, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.YEAR, FUNC_EXTRACT);
    }
    //J+
    static final IntKeyIntValueHashMap customValueFuncMap =
        new IntKeyIntValueHashMap();

    static {
        customValueFuncMap.put(Tokens.TODAY, FUNC_CURRENT_DATE);
        customValueFuncMap.put(Tokens.NOW, FUNC_LOCALTIMESTAMP);
    }

    private int                   extractSpec;
    private Pattern               pattern;
    private IntKeyIntValueHashMap charLookup;

    public static FunctionSQL newCustomFunction(Session session, String token,
            int tokenType) {

        int id = customRegularFuncMap.get(tokenType, -1);

        if (id == -1) {
            id = customValueFuncMap.get(tokenType, -1);
        }

        if (id == -1) {
            return null;
        }

        switch (tokenType) {

            case Tokens.BITLENGTH :
            case Tokens.LCASE :
            case Tokens.LENGTH :
            case Tokens.LOG :
            case Tokens.OCTETLENGTH :
            case Tokens.UCASE :
                return new FunctionSQL(id);

            case Tokens.CURDATE :
            case Tokens.CURTIME :
            case Tokens.TODAY :
            case Tokens.NOW : {
                FunctionSQL function = new FunctionSQL(id);

                function.parseList = optionalNoParamList;

                return function;
            }
            case Tokens.SUBSTR : {
                FunctionSQL function = new FunctionSQL(id);

                function.parseList = tripleParamList;
                function.name      = Tokens.T_SUBSTR;

                return function;
            }
            case Tokens.DATENAME :
            case Tokens.DATEPART :
            case Tokens.EOMONTH :
            case Tokens.NEWID :
                if (!session.database.sqlSyntaxMss) {
                    return null;
                }
                break;

            case Tokens.DBTIMEZONE :
            case Tokens.SYS_GUID :
            case Tokens.SYSTIMESTAMP :
                if (!session.database.sqlSyntaxOra) {
                    return null;
                }
        }

        FunctionCustom function = new FunctionCustom(id);

        if (id == FUNC_TRIM_CHAR) {
            switch (tokenType) {

                case Tokens.LTRIM :
                    function.extractSpec = Tokens.LEADING;
                    break;

                case Tokens.RTRIM :
                    function.extractSpec = Tokens.TRAILING;
                    break;

                default :
            }
        }

        if (id == FUNC_EXTRACT) {
            switch (tokenType) {

                case Tokens.DAYNAME :
                    function.extractSpec = Tokens.DAY_NAME;
                    break;

                case Tokens.MONTHNAME :
                    function.extractSpec = Tokens.MONTH_NAME;
                    break;

                case Tokens.DAYOFMONTH :
                    function.extractSpec = Tokens.DAY_OF_MONTH;
                    break;

                case Tokens.DAYOFWEEK :
                    function.extractSpec = Tokens.DAY_OF_WEEK;
                    break;

                case Tokens.DAYOFYEAR :
                    function.extractSpec = Tokens.DAY_OF_YEAR;
                    break;

                case Tokens.WEEK :
                    function.extractSpec = Tokens.WEEK_OF_YEAR;
                    break;

                default :
                    function.extractSpec = tokenType;
            }
        }

        if (function.name == null) {
            function.name = token;
        }

        return function;
    }

    public static boolean isRegularFunction(int tokenType) {
        return customRegularFuncMap.get(tokenType, -1) != -1;
    }

    public static boolean isValueFunction(int tokenType) {
        return customValueFuncMap.get(tokenType, -1) != -1;
    }

    private FunctionCustom(int id) {

        super();

        this.funcType   = id;
        isDeterministic = !nonDeterministicFuncSet.contains(id);

        switch (id) {

            case FUNC_SQLCODE :
            case FUNC_SQLERRM :
                parseList = optionalNoParamList;
                break;

            case FUNC_DBTIMEZONE :
            case FUNC_SESSIONTIMEZONE :
            case FUNC_SYSDATE :
            case FUNC_SYSTIMESTAMP :
                parseList = optionalNoParamList;
                break;

            case FUNC_ACTION_ID :
            case FUNC_DATABASE :
            case FUNC_DATABASE_ISOLATION_LEVEL :
            case FUNC_DATABASE_NAME :
            case FUNC_DATABASE_TIMEZONE :
            case FUNC_DATABASE_VERSION :
            case FUNC_ISAUTOCOMMIT :
            case FUNC_ISOLATION_LEVEL :
            case FUNC_ISREADONLYDATABASE :
            case FUNC_ISREADONLYDATABASEFILES :
            case FUNC_ISREADONLYSESSION :
            case FUNC_PI :
            case FUNC_SESSION_ID :
            case FUNC_SESSION_ISOLATION_LEVEL :
            case FUNC_SESSION_TIMEZONE :
            case FUNC_SYS_GUID :
            case FUNC_TIMEZONE :
            case FUNC_TRANSACTION_CONTROL :
            case FUNC_TRANSACTION_ID :
            case FUNC_TRANSACTION_SIZE :
            case FUNC_TRANSACTION_UTC :
                parseList = emptyParamList;
                break;

            case FUNC_ACOS :
            case FUNC_ASCII :
            case FUNC_ASCIISTR :
            case FUNC_ASIN :
            case FUNC_ATAN :
            case FUNC_BITNOT :
            case FUNC_CHAR :
            case FUNC_COS :
            case FUNC_COSH :
            case FUNC_COT :
            case FUNC_DEGREES :
            case FUNC_DAYS :
            case FUNC_FROM_BASE64 :
            case FUNC_HEX :
            case FUNC_HEXTORAW :
            case FUNC_LAST_DAY :
            case FUNC_LOB_ID :
            case FUNC_LOG10 :
            case FUNC_RADIANS :
            case FUNC_RAWTOHEX :
            case FUNC_REVERSE :
            case FUNC_ROUNDMAGIC :
            case FUNC_SIGN :
            case FUNC_SIN :
            case FUNC_SINH :
            case FUNC_SOUNDEX :
            case FUNC_SPACE :
            case FUNC_SYS_EXTRACT_UTC :
            case FUNC_TAN :
            case FUNC_TANH :
            case FUNC_TIMESTAMP_WITH_ZONE :
            case FUNC_TO_BASE64 :
            case FUNC_TO_DSINTERVAL :
            case FUNC_TO_YMINTERVAL :
            case FUNC_TO_NUMBER :
            case FUNC_UNHEX :
            case FUNC_UNISTR :
                parseList = singleParamList;
                break;

            case FUNC_ADD_MONTHS :
            case FUNC_ATAN2 :
            case FUNC_CONCAT :
            case FUNC_CRYPT_KEY :
            case FUNC_BITAND :
            case FUNC_BITANDNOT :
            case FUNC_BITOR :
            case FUNC_BITXOR :
            case FUNC_DIFFERENCE :
            case FUNC_FROM_TZ :
            case FUNC_LEFT :
            case FUNC_MONTHS_BETWEEN :
            case FUNC_NANVL :
            case FUNC_NEXT_DAY :
            case FUNC_NUMTODSINTERVAL :
            case FUNC_NUMTOYMINTERVAL :
            case FUNC_REGEXP_COUNT :
            case FUNC_REGEXP_INSTR :
            case FUNC_REGEXP_LIKE :
            case FUNC_REGEXP_SUBSTR :
            case FUNC_REGEXP_MATCHES :
            case FUNC_REGEXP_SUBSTRING :
            case FUNC_REGEXP_SUBSTRING_ARRAY :
            case FUNC_REPEAT :
            case FUNC_RIGHT :
                parseList = doubleParamList;
                break;

            case FUNC_TO_CHAR :
            case FUNC_TO_DATE :
            case FUNC_TO_TIMESTAMP :
            case FUNC_TO_TIMESTAMP_TZ :
                parseList = optionalDoubleParamList;
                break;

            case FUNC_LOAD_FILE :
            case FUNC_ROUND :
            case FUNC_TIMESTAMP :
            case FUNC_TRUNC :
            case FUNC_TRUNCATE :
                parseList = optionalDoubleParamList;
                break;

            case FUNC_DATEDIFF :
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.X_TOKEN, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION,
                    Tokens.CLOSEBRACKET
                };
                parseListAlt = new short[] {
                    Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.CLOSEBRACKET
                };
                break;

            case FUNC_DATEADD :
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.X_TOKEN, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION,
                    Tokens.CLOSEBRACKET
                };
                parseListAlt = tripleParamList;
                break;

            case FUNC_DATENAME :
            case FUNC_DATEPART :
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.X_TOKEN, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.CLOSEBRACKET
                };
                break;

            case FUNC_DATE_ADD :
            case FUNC_DATE_SUB :
                parseList = doubleParamList;
                break;

            case FUNC_SEQUENCE_ARRAY :
            case FUNC_TRANSLATE :
                parseList = tripleParamList;
                break;

            case FUNC_LPAD :
            case FUNC_RPAD :
            case FUNC_POSITION_CHAR :
            case FUNC_REPLACE :
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.CLOSEBRACKET
                };
                break;

            case FUNC_REGEXP_REPLACE :
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.CLOSEBRACKET
                };
                break;

            case FUNC_UNIX_MILLIS :
            case FUNC_UNIX_TIMESTAMP :
            case FUNC_UUID :
                parseList = optionalSingleParamList;
                break;

            case FUNC_EXTRACT :
                name      = Tokens.T_EXTRACT;
                parseList = singleParamList;
                break;

            case FUNC_TRIM_CHAR :
                name      = Tokens.T_TRIM;
                parseList = optionalDoubleParamList;
                break;

            case FUNC_OVERLAY_CHAR :
                name      = Tokens.T_OVERLAY;
                parseList = quadParamList;
                break;

            case FUNC_IDENTITY :
                name      = Tokens.T_IDENTITY;
                parseList = emptyParamList;
                break;

            case FUNC_DIAGNOSTICS :
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.ROW_COUNT, Tokens.CLOSEBRACKET
                };
                break;

            case FUNC_POSITION_ARRAY :
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.IN,
                    Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.FROM,
                    Tokens.QUESTION, Tokens.CLOSEBRACKET
                };
                break;

            case FUNC_SORT_ARRAY :
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.X_OPTION, 4,
                    Tokens.X_KEYSET, 2, Tokens.ASC, Tokens.DESC,
                    Tokens.X_OPTION, 5, Tokens.NULLS, Tokens.X_KEYSET, 2,
                    Tokens.FIRST, Tokens.LAST, Tokens.CLOSEBRACKET
                };
                break;

            case FUNC_TIMESTAMPADD :
                name      = Tokens.T_TIMESTAMPADD;
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.X_KEYSET, 22,
                    Tokens.SQL_TSI_FRAC_SECOND, Tokens.SQL_TSI_MICRO_SECOND,
                    Tokens.SQL_TSI_MILLI_SECOND, Tokens.SQL_TSI_SECOND,
                    Tokens.SQL_TSI_MINUTE, Tokens.SQL_TSI_HOUR,
                    Tokens.SQL_TSI_DAY, Tokens.SQL_TSI_WEEK,
                    Tokens.SQL_TSI_MONTH, Tokens.SQL_TSI_QUARTER,
                    Tokens.SQL_TSI_YEAR, Tokens.NANOSECOND, Tokens.MICROSECOND,
                    Tokens.MILLISECOND, Tokens.SECOND, Tokens.MINUTE,
                    Tokens.HOUR, Tokens.DAY, Tokens.WEEK, Tokens.MONTH,
                    Tokens.QUARTER, Tokens.YEAR, Tokens.COMMA, Tokens.QUESTION,
                    Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET
                };
                break;

            case FUNC_TIMESTAMPDIFF :
                name      = Tokens.T_TIMESTAMPDIFF;
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.X_KEYSET, 22,
                    Tokens.SQL_TSI_FRAC_SECOND, Tokens.SQL_TSI_MICRO_SECOND,
                    Tokens.SQL_TSI_MILLI_SECOND, Tokens.SQL_TSI_SECOND,
                    Tokens.SQL_TSI_MINUTE, Tokens.SQL_TSI_HOUR,
                    Tokens.SQL_TSI_DAY, Tokens.SQL_TSI_WEEK,
                    Tokens.SQL_TSI_MONTH, Tokens.SQL_TSI_QUARTER,
                    Tokens.SQL_TSI_YEAR, Tokens.NANOSECOND, Tokens.MICROSECOND,
                    Tokens.MILLISECOND, Tokens.SECOND, Tokens.MINUTE,
                    Tokens.HOUR, Tokens.DAY, Tokens.WEEK, Tokens.MONTH,
                    Tokens.QUARTER, Tokens.YEAR, Tokens.COMMA, Tokens.QUESTION,
                    Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET
                };
                break;

            case FUNC_RAND :
                parseList = optionalSingleParamList;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionCustom");
        }
    }

    public void setArguments(Expression[] nodes) {

        switch (funcType) {

            case FUNC_POSITION_CHAR : {
                Expression[] newNodes = new Expression[4];

                if (Tokens.T_LOCATE.equals(name)) {

                    // LOCATE
                    newNodes[0] = nodes[0];
                    newNodes[1] = nodes[1];
                    newNodes[3] = nodes[2];
                    nodes       = newNodes;
                } else if (Tokens.T_INSTR.equals(name)) {
                    newNodes[0] = nodes[1];
                    newNodes[1] = nodes[0];
                    newNodes[3] = nodes[2];
                    nodes       = newNodes;
                }

                break;
            }
            case FUNC_OVERLAY_CHAR : {
                Expression start  = nodes[1];
                Expression length = nodes[2];

                nodes[1] = nodes[3];
                nodes[2] = start;
                nodes[3] = length;

                break;
            }
            case FUNC_EXTRACT : {
                Expression[] newNodes = new Expression[2];

                newNodes[0] =
                    new ExpressionValue(ValuePool.getInt(extractSpec),
                                        Type.SQL_INTEGER);
                newNodes[1] = nodes[0];
                nodes       = newNodes;

                break;
            }
            case FUNC_TRIM_CHAR : {
                Expression[] newNodes = new Expression[3];

                newNodes[0] =
                    new ExpressionValue(ValuePool.getInt(extractSpec),
                                        Type.SQL_INTEGER);
                newNodes[1] = nodes[1] == null
                              ? new ExpressionValue(" ", Type.SQL_CHAR)
                              : nodes[1];
                newNodes[2] = nodes[0];
                nodes       = newNodes;
            }
        }

        super.setArguments(nodes);
    }

    public Expression getFunctionExpression() {

        switch (funcType) {

            case FUNC_CONCAT :
                return new ExpressionArithmetic(OpTypes.CONCAT,
                                                nodes[Expression.LEFT],
                                                nodes[Expression.RIGHT]);
        }

        return super.getFunctionExpression();
    }

    Object getValue(Session session, Object[] data) {

        switch (funcType) {

            case FUNC_SQLCODE :
                return 0;

            case FUNC_SQLERRM :
                return "Error";

            case FUNC_POSITION_CHAR :
            case FUNC_EXTRACT :
            case FUNC_TRIM_CHAR :
            case FUNC_OVERLAY_CHAR :
                return super.getValue(session, data);

            case FUNC_DATABASE :
                return session.getDatabase().getPath();

            case FUNC_DATABASE_NAME :
                return session.getDatabase().getNameString();

            case FUNC_ISAUTOCOMMIT :
                return session.isAutoCommit() ? Boolean.TRUE
                                              : Boolean.FALSE;

            case FUNC_ISREADONLYSESSION :
                return session.isReadOnlyDefault() ? Boolean.TRUE
                                                   : Boolean.FALSE;

            case FUNC_ISREADONLYDATABASE :
                return session.getDatabase().databaseReadOnly ? Boolean.TRUE
                                                              : Boolean.FALSE;

            case FUNC_ISREADONLYDATABASEFILES :
                return session.getDatabase().isFilesReadOnly() ? Boolean.TRUE
                                                               : Boolean.FALSE;

            case FUNC_ISOLATION_LEVEL : {
                return Session.getIsolationString(session.isolationLevel);
            }
            case FUNC_SESSION_ISOLATION_LEVEL :
                return Session.getIsolationString(
                    session.isolationLevelDefault);

            case FUNC_DATABASE_ISOLATION_LEVEL :
                return Session.getIsolationString(
                    session.database.defaultIsolationLevel);

            case FUNC_TRANSACTION_CONTROL :
                switch (session.database.txManager.getTransactionControl()) {

                    case TransactionManager.MVCC :
                        return Tokens.T_MVCC;

                    case TransactionManager.MVLOCKS :
                        return Tokens.T_MVLOCKS;

                    case TransactionManager.LOCKS :
                    default :
                        return Tokens.T_LOCKS;
                }
            case FUNC_TIMEZONE :
                return new IntervalSecondData(session.getZoneSeconds(), 0);

            case FUNC_SESSION_TIMEZONE :
                return new IntervalSecondData(session.getZoneSeconds(), 0);

            case FUNC_DBTIMEZONE : {
                TimestampData timestamp =
                    DateTimeType.newSystemTimestampWithZone();
                IntervalSecondData zone =
                    new IntervalSecondData(timestamp.getZone(), 0);

                return Type.SQL_INTERVAL_HOUR_TO_MINUTE.convertToString(zone);
            }
            case FUNC_DATABASE_TIMEZONE :
                int sec = HsqlDateTime.getZoneSeconds();

                return new IntervalSecondData(sec, 0);

            case FUNC_DATABASE_VERSION :
                return HsqlDatabaseProperties.THIS_FULL_VERSION;

            case FUNC_SESSION_ID : {
                return Long.valueOf(session.getId());
            }
            case FUNC_ACTION_ID : {
                return Long.valueOf(session.actionSCN);
            }
            case FUNC_TRANSACTION_ID : {
                return Long.valueOf(session.transactionSCN);
            }
            case FUNC_TRANSACTION_SIZE : {
                return Long.valueOf(session.getTransactionSize());
            }
            case FUNC_TRANSACTION_UTC : {
                return session.getTransactionUTC();
            }
            case FUNC_LOB_ID : {
                LobData lob = (LobData) data[0];

                if (lob == null) {
                    return null;
                }

                return Long.valueOf(lob.getId());
            }
            case FUNC_IDENTITY : {
                Number id = session.getLastIdentity();

                if (id instanceof Long) {
                    return id;
                } else {
                    return ValuePool.getLong(id.longValue());
                }
            }
            case FUNC_DIAGNOSTICS : {
                return session.sessionContext
                    .diagnosticsVariables[exprSubType];
            }
            case FUNC_SEQUENCE_ARRAY : {
                for (int i = 0; i < data.length; i++) {
                    if (data[i] == null) {
                        return null;
                    }
                }

                HsqlArrayList list    = new HsqlArrayList();
                Object        current = data[0];
                Type          type    = nodes[0].getDataType();
                boolean ascending = type.compare(session, data[1], data[0])
                                    >= 0;

                while (true) {
                    int compare = type.compare(session, current, data[1]);

                    if (ascending) {
                        if (compare > 0) {
                            break;
                        }
                    } else if (compare < 0) {
                        break;
                    }

                    list.add(current);

                    Object newValue = type.add(session, current, data[2],
                                               nodes[2].getDataType());

                    compare = type.compare(session, current, newValue);

                    if (ascending) {
                        if (compare >= 0) {
                            break;
                        }
                    } else if (compare <= 0) {
                        break;
                    }

                    current = newValue;
                }

                Object[] array = list.toArray();

                return array;
            }
            case FUNC_TIMESTAMPADD : {
                if (data[1] == null || data[2] == null) {
                    return null;
                }

                data[1] = Type.SQL_BIGINT.convertToType(session, data[1],
                        nodes[1].getDataType());

                int           part = ((Number) nodes[0].valueData).intValue();
                long          units  = ((Number) data[1]).longValue();
                TimestampData source = (TimestampData) data[2];
                IntervalType  t;
                Object        o;

                switch (part) {

                    case Tokens.NANOSECOND :
                    case Tokens.SQL_TSI_FRAC_SECOND : {
                        long seconds = units / DTIType.limitNanoseconds;
                        int  nanos = (int) (units % DTIType.limitNanoseconds);

                        t = Type
                            .SQL_INTERVAL_SECOND_MAX_FRACTION_MAX_PRECISION;
                        o = new IntervalSecondData(seconds, nanos, t);

                        return dataType.add(session, source, o, t);
                    }
                    case Tokens.MICROSECOND :
                    case Tokens.SQL_TSI_MICRO_SECOND : {
                        long seconds = units / 1000000;
                        int  nanos   = (int) (units % 1000000) * 1000;

                        t = Type
                            .SQL_INTERVAL_SECOND_MAX_FRACTION_MAX_PRECISION;
                        o = new IntervalSecondData(seconds, nanos, t);

                        return dataType.add(session, source, o, t);
                    }
                    case Tokens.MILLISECOND :
                    case Tokens.SQL_TSI_MILLI_SECOND : {
                        long seconds = units / 1000;
                        int  nanos   = (int) (units % 1000) * 1000000;

                        t = Type
                            .SQL_INTERVAL_SECOND_MAX_FRACTION_MAX_PRECISION;
                        o = new IntervalSecondData(seconds, nanos, t);

                        return dataType.add(session, source, o, t);
                    }
                    case Tokens.SECOND :
                    case Tokens.SQL_TSI_SECOND :
                        t = Type
                            .SQL_INTERVAL_SECOND_MAX_FRACTION_MAX_PRECISION;
                        o = IntervalSecondData.newIntervalSeconds(units, t);

                        return dataType.add(session, source, o, t);

                    case Tokens.MINUTE :
                    case Tokens.SQL_TSI_MINUTE :
                        t = Type.SQL_INTERVAL_MINUTE_MAX_PRECISION;
                        o = IntervalSecondData.newIntervalMinute(units, t);

                        return dataType.add(session, source, o, t);

                    case Tokens.HOUR :
                    case Tokens.SQL_TSI_HOUR :
                        t = Type.SQL_INTERVAL_HOUR_MAX_PRECISION;
                        o = IntervalSecondData.newIntervalHour(units, t);

                        return dataType.add(session, source, o, t);

                    case Tokens.DAY :
                    case Tokens.DAY_OF_YEAR :
                    case Tokens.DAY_OF_WEEK :
                    case Tokens.SQL_TSI_DAY :
                        t = Type.SQL_INTERVAL_DAY_MAX_PRECISION;
                        o = IntervalSecondData.newIntervalDay(units, t);

                        return dataType.add(session, source, o, t);

                    case Tokens.WEEK :
                    case Tokens.SQL_TSI_WEEK :
                        t = Type.SQL_INTERVAL_DAY_MAX_PRECISION;
                        o = IntervalSecondData.newIntervalDay(units * 7, t);

                        return dataType.add(session, source, o, t);

                    case Tokens.MONTH :
                    case Tokens.SQL_TSI_MONTH :
                        t = Type.SQL_INTERVAL_MONTH_MAX_PRECISION;
                        o = IntervalMonthData.newIntervalMonth(units, t);

                        return dataType.add(session, source, o, t);

                    case Tokens.QUARTER :
                    case Tokens.SQL_TSI_QUARTER :
                        t = Type.SQL_INTERVAL_MONTH_MAX_PRECISION;
                        o = IntervalMonthData.newIntervalMonth(units * 3, t);

                        return dataType.add(session, source, o, t);

                    case Tokens.YEAR :
                    case Tokens.SQL_TSI_YEAR :
                        t = Type.SQL_INTERVAL_YEAR_MAX_PRECISION;
                        o = IntervalMonthData.newIntervalMonth(units * 12, t);

                        return dataType.add(session, source, o, t);

                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "FunctionCustom");
                }
            }
            case FUNC_TIMESTAMPDIFF : {
                if (data[1] == null || data[2] == null) {
                    return null;
                }

                int           part = ((Number) nodes[0].valueData).intValue();
                TimestampData a    = (TimestampData) data[2];
                TimestampData b    = (TimestampData) data[1];
                IntervalType  t;
                long          ret;

                switch (part) {

                    case Tokens.NANOSECOND :
                    case Tokens.SQL_TSI_FRAC_SECOND : {
                        t = Type
                            .SQL_INTERVAL_SECOND_MAX_FRACTION_MAX_PRECISION;

                        IntervalSecondData interval =
                            (IntervalSecondData) t.subtract(session, a, b,
                                                            null);

                        return Long.valueOf(
                            DTIType.limitNanoseconds * interval.getSeconds()
                            + interval.getNanos());
                    }
                    case Tokens.MICROSECOND :
                    case Tokens.SQL_TSI_MICRO_SECOND : {
                        t = Type.SQL_INTERVAL_SECOND_MAX_PRECISION;

                        IntervalSecondData interval =
                            (IntervalSecondData) t.subtract(session, a, b,
                                                            null);

                        return Long.valueOf(1000000 * interval.getSeconds()
                                            + interval.getNanos() / 1000);
                    }
                    case Tokens.MILLISECOND :
                    case Tokens.SQL_TSI_MILLI_SECOND : {
                        t = Type.SQL_INTERVAL_SECOND_MAX_PRECISION;

                        IntervalSecondData interval =
                            (IntervalSecondData) t.subtract(session, a, b,
                                                            null);

                        return Long.valueOf(1000 * interval.getSeconds()
                                            + interval.getNanos() / 1000000);
                    }
                    case Tokens.SECOND :
                    case Tokens.SQL_TSI_SECOND :
                        t = Type.SQL_INTERVAL_SECOND_MAX_PRECISION;

                        return Long.valueOf(
                            t.convertToLongEndUnits(
                                t.subtract(session, a, b, null)));

                    case Tokens.MINUTE :
                    case Tokens.SQL_TSI_MINUTE :
                        t = Type.SQL_INTERVAL_MINUTE_MAX_PRECISION;

                        return Long.valueOf(
                            t.convertToLongEndUnits(
                                t.subtract(session, a, b, null)));

                    case Tokens.HOUR :
                    case Tokens.SQL_TSI_HOUR :
                        t = Type.SQL_INTERVAL_HOUR_MAX_PRECISION;

                        return Long.valueOf(
                            t.convertToLongEndUnits(
                                t.subtract(session, a, b, null)));

                    case Tokens.DAY :
                    case Tokens.SQL_TSI_DAY :
                        t = Type.SQL_INTERVAL_DAY_MAX_PRECISION;

                        return Long.valueOf(
                            t.convertToLongEndUnits(
                                t.subtract(session, a, b, null)));

                    case Tokens.WEEK :
                    case Tokens.SQL_TSI_WEEK :
                        t = Type.SQL_INTERVAL_DAY_MAX_PRECISION;
                        ret = t.convertToLongEndUnits(t.subtract(session, a,
                                b, null));
                        ret = ret < 0 ? (ret - 6) / 7
                                      : (ret + 6) / 7;

                        return Long.valueOf(ret);

                    case Tokens.MONTH :
                    case Tokens.SQL_TSI_MONTH :
                        t = Type.SQL_INTERVAL_MONTH_MAX_PRECISION;

                        return Long.valueOf(
                            t.convertToLongEndUnits(
                                t.subtract(session, a, b, null)));

                    case Tokens.QUARTER :
                    case Tokens.SQL_TSI_QUARTER :
                        t = Type.SQL_INTERVAL_MONTH_MAX_PRECISION;
                        ret = t.convertToLongEndUnits(t.subtract(session, a,
                                b, null));
                        ret = ret < 0 ? (ret - 2) / 3
                                      : (ret + 2) / 3;

                        return Long.valueOf(ret);

                    case Tokens.YEAR :
                    case Tokens.SQL_TSI_YEAR :
                        t = Type.SQL_INTERVAL_YEAR_MAX_PRECISION;

                        return Long.valueOf(
                            t.convertToLongEndUnits(
                                t.subtract(session, a, b, null)));

                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "FunctionCustom");
                }
            }
            case FUNC_DATENAME : {
                Object value = getExtractValue(session, data);

                // todo - convert WEEKDAY to Monday, .., MONTH to October
                return dataType.convertToDefaultType(session, value);
            }
            case FUNC_DATE_ADD : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                return dataType.add(session, data[0], data[1],
                                    nodes[RIGHT].dataType);
            }
            case FUNC_DATE_SUB : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                return dataType.subtract(session, data[0], data[1],
                                         nodes[RIGHT].dataType);
            }
            case FUNC_DAYS : {
                if (data[0] == null) {
                    return null;
                }

                IntervalSecondData diff =
                    (IntervalSecondData) Type.SQL_INTERVAL_DAY_MAX_PRECISION
                        .subtract(session, data[0],
                                  DateTimeType.epochTimestamp, Type.SQL_DATE);

                return ValuePool.getInt((int) (diff.getSeconds()
                                               / (24 * 60 * 60) + 1));
            }
            case FUNC_PI :
                return Double.valueOf(Math.PI);

            case FUNC_RAND : {
                if (nodes[0] == null) {
                    return Double.valueOf(session.random());
                } else {
                    data[0] = Type.SQL_BIGINT.convertToType(session, data[0],
                            nodes[0].getDataType());

                    long seed = ((Number) data[0]).longValue();

                    return Double.valueOf(session.random(seed));
                }
            }
            case FUNC_ROUND :
            case FUNC_TRUNC : {
                int interval = Types.SQL_INTERVAL_DAY;

                if (data[0] == null) {
                    return null;
                }

                if (dataType.isDateTimeType()) {
                    DateTimeType type = (DateTimeType) dataType;

                    if (nodes.length > 1 && nodes[1] != null) {
                        if (data[1] == null) {
                            return null;
                        }

                        interval = HsqlDateTime.toStandardIntervalPart(
                            (String) data[1]);
                    }

                    if (interval < 0) {
                        throw Error.error(ErrorCode.X_42566, (String) data[1]);
                    }

                    return funcType == FUNC_ROUND
                           ? type.round(session, data[0], interval)
                           : type.truncate(session, data[0], interval);
                }
            }

            // fall through
            case FUNC_TRUNCATE : {
                int offset = 0;

                if (data[0] == null) {
                    return null;
                }

                if (nodes.length > 1) {
                    if (data[1] == null) {
                        return null;
                    }

                    data[1] = Type.SQL_INTEGER.convertToType(session, data[1],
                            nodes[1].getDataType());
                    offset = ((Number) data[1]).intValue();
                }

                return funcType == FUNC_ROUND
                       ? ((NumberType) dataType).round(data[0], offset)
                       : ((NumberType) dataType).truncate(data[0], offset);
            }
            case FUNC_FROM_BASE64 : {

//#ifdef JAVA8
                String val = (String) data[0];

                if (val == null) {
                    return null;
                }

                val = val.trim();

                byte[] bytes = java.util.Base64.getDecoder().decode(val);

                return new BinaryData(bytes, false);

//#else
/*
                throw Error.error(ErrorCode.X_0A501);
*/

//#endif JAVA8
            }
            case FUNC_TO_BASE64 : {

//#ifdef JAVA8
                BinaryData val = (BinaryData) data[0];

                if (val == null) {
                    return null;
                }

                return java.util.Base64.getEncoder().encodeToString(
                    val.getBytes());

//#else
/*
                throw Error.error(ErrorCode.X_0A501);
*/

//#endif JAVA8
            }
            case FUNC_TO_CHAR : {
                if (nodes[1] == null) {
                    return dataType.convertToType(session, data[0],
                                                  nodes[0].dataType);
                } else {
                    if (data[0] == null || data[1] == null) {
                        return null;
                    }

                    SimpleDateFormat format = session.getSimpleDateFormatGMT();
                    Date date =
                        (Date) ((DateTimeType) nodes[0].dataType)
                            .convertSQLToJavaGMT(session, data[0]);

                    return HsqlDateTime.toFormattedDate(date,
                                                        (String) data[1],
                                                        format);
                }
            }
            case FUNC_TO_NUMBER : {
                if (data[0] == null) {
                    return null;
                }

                return dataType.convertToType(session, data[0],
                                              nodes[0].dataType);
            }
            case FUNC_TO_DATE :
            case FUNC_TO_TIMESTAMP : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                if (nodes[0].dataType.isDateOrTimestampType()) {
                    return dataType.convertToType(session, data[0],
                                                  nodes[0].dataType);
                }

                boolean          fraction = funcType == FUNC_TO_TIMESTAMP;
                SimpleDateFormat format   = session.getSimpleDateFormatGMT();
                TimestampData value = HsqlDateTime.toDate((String) data[0],
                    (String) data[1], format, fraction);

                return value;
            }
            case FUNC_TIMESTAMP : {
                boolean unary = nodes[1] == null;

                if (data[0] == null) {
                    return null;
                }

                if (unary) {
                    if (nodes[0].dataType.isNumberType()) {
                        Calendar calendar = session.getCalendar();
                        long     seconds  = ((Number) data[0]).longValue();

                        calendar.setTimeInMillis(seconds * 1000);

                        int zone = HsqlDateTime.getZoneSeconds(calendar);

                        return new TimestampData(seconds + zone);
                    }

                    try {
                        return Type.SQL_TIMESTAMP.convertToType(session,
                                data[0], nodes[0].dataType);
                    } catch (HsqlException e) {
                        return Type.SQL_DATE.convertToType(session, data[0],
                                                           nodes[0].dataType);
                    }
                }

                if (data[1] == null) {
                    return null;
                }

                TimestampData date =
                    (TimestampData) Type.SQL_DATE.convertToType(session,
                        data[0], nodes[0].dataType);
                TimeData time =
                    (TimeData) Type.SQL_TIME_MAX.convertToType(session,
                        data[1], nodes[1].dataType);

                return new TimestampData(date.getSeconds()
                                         + time.getSeconds(), time.getNanos());
            }
            case FUNC_TIMESTAMP_WITH_ZONE : {
                Calendar calendar = session.getCalendar();
                long     seconds;
                int      nanos = 0;
                int      zone;

                if (data[0] == null) {
                    return null;
                }

                if (nodes[0].dataType.isNumberType()) {
                    seconds = ((Number) data[0]).longValue();
                } else if (nodes[0].dataType.typeCode == Types.SQL_TIMESTAMP) {
                    seconds = ((TimestampData) data[0]).getSeconds();
                    seconds =
                        HsqlDateTime.convertMillisToCalendar(
                            calendar, seconds * 1000) / 1000;
                } else if (nodes[0].dataType.typeCode
                           == Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
                    seconds = ((TimestampData) data[0]).getSeconds();
                } else {
                    throw Error.error(ErrorCode.X_42566, (String) data[1]);
                }

                calendar.setTimeInMillis(seconds * 1000);

                zone = HsqlDateTime.getZoneSeconds(calendar);

                return new TimestampData(seconds, nanos, zone);
            }
            case FUNC_SYS_GUID : {
                UUID uuid = java.util.UUID.randomUUID();
                long hi   = uuid.getMostSignificantBits();
                long lo   = uuid.getLeastSignificantBits();

                return new BinaryData(ArrayUtil.toByteArray(hi, lo), false);
            }
            case FUNC_UUID : {
                if (nodes[0] == null) {
                    UUID uuid = java.util.UUID.randomUUID();
                    long hi   = uuid.getMostSignificantBits();
                    long lo   = uuid.getLeastSignificantBits();

                    return new BinaryData(ArrayUtil.toByteArray(hi, lo),
                                          false);
                } else {
                    if (data[0] == null) {
                        return null;
                    }

                    try {
                        if (dataType.isBinaryType()) {
                            byte[] bytes =
                                StringConverter.toBinaryUUID((String) data[0]);

                            return new BinaryData(bytes, false);
                        } else {
                            return StringConverter.toStringUUID(
                                ((BinaryData) data[0]).getBytes());
                        }
                    } catch (NumberFormatException e) {
                        throw Error.error(ErrorCode.X_22026, e);
                    }
                }
            }
            case FUNC_UNIX_MILLIS : {
                TimestampData ts;

                if (nodes[0] == null) {
                    ts = session.getCurrentTimestamp();
                } else {
                    if (data[0] == null) {
                        return null;
                    }

                    ts = (TimestampData) data[0];
                }

                long millis = ts.getSeconds() * 1000 + ts.getNanos() / 1000000;

                return Long.valueOf(millis);
            }
            case FUNC_UNIX_TIMESTAMP : {
                TimestampData ts;

                if (nodes[0] == null) {
                    ts = session.getCurrentTimestamp();
                } else {
                    if (data[0] == null) {
                        return null;
                    }

                    ts = (TimestampData) data[0];
                }

                return Long.valueOf(ts.getSeconds());
            }
            case FUNC_ACOS : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.acos(d));
            }
            case FUNC_ASIN : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.asin(d));
            }
            case FUNC_ATAN : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.atan(d));
            }
            case FUNC_COS : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.cos(d));
            }
            case FUNC_COSH : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.cosh(d));
            }
            case FUNC_COT : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);
                double c = 1.0 / java.lang.Math.tan(d);

                return Double.valueOf(c);
            }
            case FUNC_DEGREES : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.toDegrees(d));
            }
            case FUNC_SIN : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.sin(d));
            }
            case FUNC_SINH : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.sinh(d));
            }
            case FUNC_TAN : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.tan(d));
            }
            case FUNC_TANH : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.tanh(d));
            }
            case FUNC_LOG10 : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                if (d < 0) {
                    if (session.database.sqlDoubleNaN) {
                        throw Error.error(ErrorCode.X_2201E);
                    }
                }

                return Double.valueOf(java.lang.Math.log10(d));
            }
            case FUNC_RADIANS : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.toRadians(d));
            }

            //
            case FUNC_SIGN : {
                if (data[0] == null) {
                    return null;
                }

                if (session.database.sqlSyntaxOra) {
                    if (data[0] instanceof Double) {
                        if (((Double) data[0]).doubleValue() == 0d) {
                            return Integer.valueOf(1);
                        }
                    }
                }

                int val =
                    ((NumberType) nodes[0].dataType).compareToZero(data[0]);

                return Integer.valueOf(val);
            }
            case FUNC_ATAN2 : {
                if (data[0] == null) {
                    return null;
                }

                double a = NumberType.toDouble(data[0]);
                double b = NumberType.toDouble(data[1]);

                return Double.valueOf(java.lang.Math.atan2(a, b));
            }
            case FUNC_ASCII : {
                String arg;

                if (data[0] == null) {
                    return null;
                }

                if (nodes[0].dataType.isLobType()) {
                    arg = ((ClobData) data[0]).getSubString(session, 0, 1);
                } else {
                    arg = (String) data[0];
                }

                if (arg.isEmpty()) {
                    return null;
                }

                return ValuePool.getInt(arg.charAt(0));
            }
            case FUNC_ASCIISTR : {
                String arg;

                if (data[0] == null) {
                    return null;
                }

                if (nodes[0].dataType.isLobType()) {
                    arg = ((ClobData) data[0]).getSubString(session, 0, 1);
                } else {
                    arg = (String) data[0];
                }

                return StringConverter.stringToUnicodeEscaped(arg);
            }
            case FUNC_CHAR :
                if (data[0] == null) {
                    return null;
                }

                data[0] = Type.SQL_INTEGER.convertToType(session, data[0],
                        nodes[0].getDataType());

                int arg = ((Number) data[0]).intValue();

                if (Character.isValidCodePoint(arg)
                        && Character.isValidCodePoint((char) arg)) {
                    return String.valueOf((char) arg);
                }

                throw Error.error(ErrorCode.X_22511);
            case FUNC_ROUNDMAGIC : {
                int offset = 0;

                if (data[0] == null) {
                    return null;
                }

                if (nodes.length > 1) {
                    if (data[1] == null) {
                        return null;
                    }

                    offset = ((Number) data[1]).intValue();
                }

                return ((NumberType) dataType).round(data[0], offset);
            }
            case FUNC_SOUNDEX : {
                if (data[0] == null) {
                    return null;
                }

                String s = (String) data[0];

                return new String(soundex(s), 0, 4);
            }
            case FUNC_BITAND :
            case FUNC_BITANDNOT :
            case FUNC_BITNOT :
            case FUNC_BITOR :
            case FUNC_BITXOR : {
                for (int i = 0; i < data.length; i++) {
                    if (data[i] == null) {
                        return null;
                    }
                }

                if (dataType.isNumberType()) {
                    long v = 0;
                    long a;
                    long b = 0;

                    data[0] = Type.SQL_BIGINT.convertToType(session, data[0],
                            nodes[0].getDataType());
                    a = ((Number) data[0]).longValue();

                    if (funcType != FUNC_BITNOT) {
                        data[1] = Type.SQL_BIGINT.convertToType(session,
                                data[1], nodes[1].getDataType());
                        b = ((Number) data[1]).longValue();
                    }

                    switch (funcType) {

                        case FUNC_BITAND :
                            v = a & b;
                            break;

                        case FUNC_BITANDNOT :
                            v = a & ~b;
                            break;

                        case FUNC_BITNOT :
                            v = ~a;
                            break;

                        case FUNC_BITOR :
                            v = a | b;
                            break;

                        case FUNC_BITXOR :
                            v = a ^ b;
                            break;

                        default :
                    }

                    switch (dataType.typeCode) {

                        case Types.SQL_NUMERIC :
                        case Types.SQL_DECIMAL :
                            return BigDecimal.valueOf(v);

                        case Types.SQL_BIGINT :
                            return ValuePool.getLong(v);

                        case Types.SQL_INTEGER :
                        case Types.SQL_SMALLINT :
                        case Types.TINYINT :
                            return ValuePool.getInt((int) v);

                        default :
                            throw Error.error(ErrorCode.X_42561);
                    }
                } else {
                    byte[] a = ((BinaryData) data[0]).getBytes();
                    byte[] b = null;
                    byte[] v;

                    if (funcType != FUNC_BITNOT) {
                        b = ((BinaryData) data[1]).getBytes();
                    }

                    switch (funcType) {

                        case FUNC_BITAND :
                            v = BitMap.and(a, b);
                            break;

                        case FUNC_BITANDNOT :
                            b = BitMap.not(b);
                            v = BitMap.and(a, b);
                            break;

                        case FUNC_BITNOT :
                            v = BitMap.not(a);
                            break;

                        case FUNC_BITOR :
                            v = BitMap.or(a, b);
                            break;

                        case FUNC_BITXOR :
                            v = BitMap.xor(a, b);
                            break;

                        default :
                            throw Error.error(ErrorCode.X_42561);
                    }

                    return new BinaryData(v, dataType.precision);
                }
            }
            case FUNC_DIFFERENCE : {
                for (int i = 0; i < data.length; i++) {
                    if (data[i] == null) {
                        return null;
                    }
                }

                char[] s1 = soundex((String) data[0]);
                char[] s2 = soundex((String) data[1]);
                int    e  = 0;

                if (s1[0] == s2[0]) {
                    e++;
                }

                int js = 1;

                for (int i = 1; i < 4; i++) {
                    for (int j = js; j < 4; j++) {
                        if (s1[j] == s2[i]) {
                            e++;

                            js = j + 1;

                            break;
                        }
                    }
                }

                return ValuePool.getInt(e);
            }
            case FUNC_HEX : {
                if (data[0] == null) {
                    return null;
                }

                if (nodes[0].dataType.isBinaryType()) {
                    return dataType.convertToType(session, data[0],
                                                  nodes[0].dataType);
                } else if (nodes[0].dataType.isNumberType()) {
                    long val = ((Number) data[0]).longValue();

                    return Long.toHexString(val);
                } else {
                    String val   = ((String) data[0]);
                    byte[] bytes = val.getBytes(JavaSystem.CS_ISO_8859_1);

                    return StringConverter.byteArrayToHexString(bytes);
                }
            }
            case FUNC_UNISTR :
                if (data[0] == null) {
                    return null;
                }

                try {
                    return StringConverter.unicodeEscapedToString(
                        (String) data[0]);
                } catch (Throwable e) {
                    throw Error.error(e, ErrorCode.X_42586, null);
                }
            case FUNC_UNHEX :
            case FUNC_HEXTORAW : {
                if (data[0] == null) {
                    return null;
                }

                return dataType.convertToType(session, data[0],
                                              nodes[0].dataType);
            }
            case FUNC_RAWTOHEX : {
                if (data[0] == null) {
                    return null;
                }

                BlobData binary = (BlobData) data[0];
                byte[] bytes = binary.getBytes(session, 0,
                                               (int) binary.length(session));

                return StringConverter.byteArrayToHexString(bytes);
            }
            case FUNC_REPEAT : {
                for (int i = 0; i < data.length; i++) {
                    if (data[i] == null) {
                        return null;
                    }
                }

                data[1] = Type.SQL_INTEGER.convertToType(session, data[1],
                        nodes[1].getDataType());

                String        string = (String) data[0];
                int           i      = ((Number) data[1]).intValue();
                StringBuilder sb     = new StringBuilder(string.length() * i);

                while (i-- > 0) {
                    sb.append(string);
                }

                return sb.toString();
            }
            case FUNC_REPLACE : {
                for (int i = 0; i < data.length; i++) {
                    if (data[i] == null) {
                        return null;
                    }
                }

                String        string  = (String) data[0];
                String        find    = (String) data[1];
                String        replace = (String) data[2];
                StringBuilder sb      = new StringBuilder();
                int           start   = 0;

                if (find.isEmpty()) {
                    return string;
                }

                while (true) {
                    int i = string.indexOf(find, start);

                    if (i == -1) {
                        sb.append(string.substring(start));

                        break;
                    }

                    sb.append(string, start, i);
                    sb.append(replace);

                    start = i + find.length();
                }

                return sb.toString();
            }
            case FUNC_LEFT :
            case FUNC_RIGHT : {
                for (int i = 0; i < data.length; i++) {
                    if (data[i] == null) {
                        return null;
                    }
                }

                int     start     = 0;
                int     count     = ((Number) data[1]).intValue();
                boolean hasLength = true;
                boolean trailing  = false;

                if (funcType == FUNC_RIGHT) {
                    start     = count;
                    count     = 0;
                    hasLength = false;
                    trailing  = true;
                }

                return ((CharacterType) dataType).substring(session, data[0],
                        start, count, hasLength, trailing);
            }
            case FUNC_SPACE : {
                if (data[0] == null) {
                    return null;
                }

                data[0] = Type.SQL_INTEGER.convertToType(session, data[0],
                        nodes[0].getDataType());

                int    count = ((Number) data[0]).intValue();
                char[] array = new char[count];

                ArrayUtil.fillArray(array, 0, ' ');

                return String.valueOf(array);
            }
            case FUNC_REVERSE : {
                if (data[0] == null) {
                    return null;
                }

                StringBuilder sb = new StringBuilder((String) data[0]);

                sb = sb.reverse();

                return sb.toString();
            }
            case FUNC_REGEXP_REPLACE : {
                for (int i = 0; i < nodes.length; i++) {
                    if (nodes[i] == null) {
                        break;
                    }

                    if (data[i] == null) {
                        return null;
                    }
                }

                int     flags = FunctionCustom.regexpParams((String) data[5]);
                Pattern currentPattern;
                String  source       = (String) data[0];
                String  matchPattern = (String) data[1];
                String  replace      = (String) data[2];
                int     start        = 1;
                int     count        = 0;

                if (nodes[2] == null) {
                    replace = "";
                }

                if (nodes[3] != null) {
                    start = ((Number) data[3]).intValue();

                    if (start < 1 || start >= source.length()) {
                        throw Error.error(ErrorCode.X_22003);
                    }
                }

                if (nodes[4] != null) {
                    count = ((Number) data[4]).intValue();

                    if (count < 0) {
                        throw Error.error(ErrorCode.X_22003);
                    }
                }

                currentPattern = pattern;

                if (currentPattern == null) {
                    currentPattern = Pattern.compile(matchPattern, flags);
                }

                Matcher matcher = currentPattern.matcher(source);
                String  result;

                if (start > 1) {
                    matcher.region(start - 1, source.length());
                }

                if (count == 0) {
                    result = matcher.replaceAll(replace);
                } else if (count == 1) {
                    result = matcher.replaceFirst(replace);
                } else {
                    throw Error.error(ErrorCode.X_22511, "count");
                }

                return result;
            }
            case FUNC_REGEXP_COUNT :
            case FUNC_REGEXP_INSTR :
            case FUNC_REGEXP_LIKE :
            case FUNC_REGEXP_SUBSTR :
            case FUNC_REGEXP_MATCHES :
            case FUNC_REGEXP_SUBSTRING :
            case FUNC_REGEXP_SUBSTRING_ARRAY : {
                for (int i = 0; i < data.length; i++) {
                    if (data[i] == null) {
                        return null;
                    }
                }

                Pattern currentPattern = pattern;
                String  source         = (String) data[0];
                String  matchPattern   = (String) data[1];

                if (currentPattern == null) {
                    currentPattern = Pattern.compile(matchPattern);
                }

                Matcher matcher = currentPattern.matcher(source);

                switch (funcType) {

                    case FUNC_REGEXP_LIKE :
                    case FUNC_REGEXP_MATCHES : {
                        boolean match = matcher.matches();

                        return Boolean.valueOf(match);
                    }
                    case FUNC_REGEXP_INSTR : {
                        boolean match = matcher.find();

                        if (match) {
                            return Integer.valueOf(matcher.start() + 1);
                        }

                        return ValuePool.INTEGER_0;
                    }
                    case FUNC_REGEXP_SUBSTR :
                    case FUNC_REGEXP_SUBSTRING : {
                        boolean match = matcher.find();

                        if (match) {
                            return matcher.group();
                        }

                        return null;
                    }
                    case FUNC_REGEXP_COUNT : {
                        int count = 0;

                        while (matcher.find()) {
                            count++;
                        }

                        return Integer.valueOf(count);
                    }
                    case FUNC_REGEXP_SUBSTRING_ARRAY : {
                        HsqlArrayList list = new HsqlArrayList();

                        while (matcher.find()) {
                            list.add(matcher.group());
                        }

                        return list.toArray();
                    }
                    default : {
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "FunctionCustom");
                    }
                }
            }
            case FUNC_CRYPT_KEY : {
                byte[] bytes = Crypto.getNewKey((String) data[0],
                                                (String) data[1]);

                return StringConverter.byteArrayToHexString(bytes);
            }
            case FUNC_LOAD_FILE : {
                String fileName = (String) data[0];

                if (fileName == null) {
                    return null;
                }

                switch (dataType.typeCode) {

                    case Types.SQL_CLOB :
                        return session.sessionData.createClobFromFile(fileName,
                                (String) data[1]);

                    case Types.SQL_BLOB :
                    default :
                        return session.sessionData.createBlobFromFile(
                            fileName);
                }
            }
            case FUNC_LPAD :
            case FUNC_RPAD : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                String value;

                if (nodes[0].dataType.typeCode == Types.SQL_CLOB) {
                    value = (String) Type.SQL_VARCHAR.convertToType(session,
                            data[0], nodes[0].dataType);
                } else if (nodes[0].dataType.isCharacterType()) {
                    value = (String) data[0];
                } else {
                    value = nodes[0].dataType.convertToString(data[0]);
                }

                int length = ((Integer) Type.SQL_INTEGER.convertToType(session,
                    data[1], nodes[1].dataType)).intValue();
                String pad = " ";

                if (nodes[2] != null) {
                    pad = nodes[2].dataType.convertToString(data[2]);

                    if (pad.isEmpty()) {
                        pad = " ";
                    }
                }

                value = (String) Type.SQL_VARCHAR.trim(session, value, ' ',
                                                       true, true);
                value = StringUtil.toPaddedString(value, length, pad,
                                                  funcType == FUNC_RPAD);

                if (dataType.isLobType()) {
                    return dataType.convertToType(session, value,
                                                  Type.SQL_VARCHAR);
                } else {
                    return value;
                }
            }
            case FUNC_POSITION_ARRAY : {
                if (data[0] == null) {
                    return null;
                }

                if (data[1] == null) {
                    return null;
                }

                Object[]  array       = (Object[]) data[1];
                ArrayType dt          = (ArrayType) nodes[1].dataType;
                Type      elementType = dt.collectionBaseType();
                int       start       = 1;

                if (nodes[2] != null) {
                    if (data[2] == null) {
                        return null;
                    }

                    start = ((Number) Type.SQL_INTEGER.convertToType(session,
                            data[2], nodes[2].dataType)).intValue();
                }

                if (start < 1) {
                    throw Error.error(ErrorCode.X_22003);
                }

                start--;

                for (int i = start; i < array.length; i++) {
                    if (elementType.compare(session, data[0], array[i]) == 0) {
                        return ValuePool.getInt(i + 1);
                    }
                }

                return ValuePool.INTEGER_0;
            }
            case FUNC_SORT_ARRAY : {
                if (data[0] == null) {
                    return null;
                }

                ArrayType    dt       = (ArrayType) dataType;
                SortAndSlice exprSort = new SortAndSlice();

                exprSort.prepareSingleColumn(1);

                exprSort.sortDescending[0] = ((Number) data[1]).intValue()
                                             == Tokens.DESC;
                exprSort.sortNullsLast[0] = ((Number) data[2]).intValue()
                                            == Tokens.LAST;

                Object[] array = (Object[]) ArrayUtil.duplicateArray(data[0]);

                dt.sort(session, array, exprSort);

                return array;
            }
            case FUNC_ADD_MONTHS : {
                if (data[0] == null) {
                    return null;
                }

                if (data[1] == null) {
                    return null;
                }

                TimestampData ts     = (TimestampData) data[0];
                int           months = ((Number) data[1]).intValue();

                return Type.SQL_TIMESTAMP_NO_FRACTION.addMonthsSpecial(session,
                        ts, months);
            }
            case FUNC_FROM_TZ : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                TimestampData timestamp = (TimestampData) data[0];

                if (DateTimeType.zoneIDs.contains(data[1])) {
                    return ((DateTimeType) dataType).changeZone(session,
                            timestamp, (String) data[1]);
                } else {
                    IntervalSecondData zone =
                        (IntervalSecondData) Type.SQL_INTERVAL_HOUR_TO_MINUTE
                            .convertToDefaultType(session, data[1]);

                    return new TimestampData(
                        timestamp.getSeconds() + timestamp.getZone()
                        - zone.getSeconds(), timestamp.getNanos(),
                                             (int) zone.getSeconds());
                }
            }
            case FUNC_LAST_DAY : {
                if (data[0] == null) {
                    return null;
                }

                return Type.SQL_TIMESTAMP_NO_FRACTION.getLastDayOfMonth(
                    session, data[0]);
            }
            case FUNC_MONTHS_BETWEEN : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                return DateTimeType.subtractMonthsSpecial(session,
                        (TimestampData) data[0], (TimestampData) data[1]);
            }
            case FUNC_NANVL :
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                if (Double.isNaN(d)) {
                    return dataType.convertToType(session, data[1],
                                                  nodes[1].dataType);
                }

                return data[0];

            case FUNC_NEXT_DAY : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                TimestampData date =
                    (TimestampData) dataType.convertToType(session, data[0],
                        nodes[0].dataType);
                int day = DateTimeType.getDayOfWeek((String) data[1]);

                return DateTimeType.nextDayOfWeek(session, date, day);
            }
            case FUNC_NUMTODSINTERVAL : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                Object st = Type.SQL_VARCHAR.trim(session, data[1], ' ', true,
                                                  true);

                st = Type.SQL_VARCHAR.upper(session, st);
                st = Type.SQL_VARCHAR.convertToDefaultType(session, st);

                int token    = Tokens.get((String) st);
                int typeCode = IntervalType.getFieldNameTypeForToken(token);

                switch (typeCode) {

                    case Types.SQL_INTERVAL_DAY :
                    case Types.SQL_INTERVAL_HOUR :
                    case Types.SQL_INTERVAL_MINUTE :
                    case Types.SQL_INTERVAL_SECOND :
                        break;

                    default :
                        throw Error.error(ErrorCode.X_42566);
                }

                double value = ((Number) data[0]).doubleValue();

                return IntervalSecondData.newInterval(value, typeCode);
            }
            case FUNC_NUMTOYMINTERVAL : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                Object st = Type.SQL_VARCHAR.trim(session, data[1], ' ', true,
                                                  true);

                st = Type.SQL_VARCHAR.upper(session, st);
                st = Type.SQL_VARCHAR.convertToDefaultType(session, st);

                int token    = Tokens.get((String) st);
                int typeCode = IntervalType.getFieldNameTypeForToken(token);

                switch (typeCode) {

                    case Types.SQL_INTERVAL_YEAR :
                    case Types.SQL_INTERVAL_MONTH :
                        break;

                    default :
                        throw Error.error(ErrorCode.X_42566);
                }

                double value = ((Number) data[0]).doubleValue();

                return IntervalMonthData.newInterval(value, typeCode);
            }
            case FUNC_SESSIONTIMEZONE : {
                IntervalSecondData zone =
                    new IntervalSecondData(session.getZoneSeconds(), 0);

                return Type.SQL_INTERVAL_HOUR_TO_MINUTE.convertToString(zone);
            }
            case FUNC_SYS_EXTRACT_UTC : {
                if (data[0] == null) {
                    return null;
                }

                return Type.SQL_TIMESTAMP_WITH_TIME_ZONE.changeZone(session,
                        data[0], Type.SQL_TIMESTAMP_WITH_TIME_ZONE, 0, false);
            }
            case FUNC_SYSDATE : {
                TimestampData timestamp = DateTimeType.newSysDateTimestamp();

                return Type.SQL_TIMESTAMP_NO_FRACTION.convertToType(session,
                        timestamp, Type.SQL_TIMESTAMP);
            }
            case FUNC_SYSTIMESTAMP : {
                return DateTimeType.newSystemTimestampWithZone();
            }
            case FUNC_TO_DSINTERVAL : {
                if (data[0] == null) {
                    return null;
                }

                return Type.SQL_INTERVAL_DAY_TO_SECOND_MAX_PRECISION
                    .convertToType(session, data[0], Type.SQL_VARCHAR);
            }
            case FUNC_TO_YMINTERVAL : {
                if (data[0] == null) {
                    return null;
                }

                return Type.SQL_INTERVAL_YEAR_TO_MONTH_MAX_PRECISION
                    .convertToType(session, data[0], Type.SQL_VARCHAR);
            }
            case FUNC_TO_TIMESTAMP_TZ : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }
            }
            case FUNC_TRANSLATE : {
                if (data[0] == null || data[1] == null || data[2] == null) {
                    return null;
                }

                IntKeyIntValueHashMap map = charLookup;

                if (map == null) {
                    map = getTranslationMap((String) data[1],
                                            (String) data[2]);
                }

                return translateWithMap((String) data[0], map);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionCustom");
        }
    }

    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        switch (funcType) {

            case FUNC_SQLCODE :
                dataType = Type.SQL_INTEGER;

                return;

            case FUNC_SQLERRM :
                dataType = Type.SQL_VARCHAR_DEFAULT;

                return;

            case FUNC_POSITION_CHAR :
            case FUNC_EXTRACT :
            case FUNC_TRIM_CHAR :
            case FUNC_OVERLAY_CHAR :
                super.resolveTypes(session, parent);

                return;

            case FUNC_DATABASE :
                dataType = Type.SQL_VARCHAR_DEFAULT;

                return;

            case FUNC_DATABASE_NAME :
                dataType = Type.SQL_VARCHAR_DEFAULT;

                return;

            case FUNC_ISAUTOCOMMIT :
            case FUNC_ISREADONLYSESSION :
            case FUNC_ISREADONLYDATABASE :
            case FUNC_ISREADONLYDATABASEFILES :
                dataType = Type.SQL_BOOLEAN;

                return;

            case FUNC_ISOLATION_LEVEL :
            case FUNC_SESSION_ISOLATION_LEVEL :
            case FUNC_DATABASE_ISOLATION_LEVEL :
            case FUNC_TRANSACTION_CONTROL :
            case FUNC_DATABASE_VERSION :
                dataType = Type.SQL_VARCHAR_DEFAULT;

                return;

            case FUNC_TIMEZONE :
            case FUNC_SESSION_TIMEZONE :
            case FUNC_DATABASE_TIMEZONE :
                dataType = Type.SQL_INTERVAL_HOUR_TO_MINUTE;

                return;

            case FUNC_SESSION_ID :
            case FUNC_ACTION_ID :
            case FUNC_TRANSACTION_ID :
            case FUNC_TRANSACTION_SIZE :
            case FUNC_LOB_ID :
            case FUNC_IDENTITY :
                dataType = Type.SQL_BIGINT;

                return;

            case FUNC_DIAGNOSTICS : {
                exprSubType = ExpressionColumn.idx_row_count;
                dataType    = Type.SQL_INTEGER;

                return;
            }
            case FUNC_SEQUENCE_ARRAY : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = nodes[1].dataType;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = nodes[0].dataType;
                }

                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_INTEGER;
                    nodes[1].dataType = Type.SQL_INTEGER;
                }

                if (nodes[0].dataType.isNumberType()) {
                    if (nodes[2].dataType == null) {
                        nodes[2].dataType = nodes[0].dataType;
                    }
                } else if (nodes[0].dataType.isDateTimeType()) {
                    if (nodes[0].dataType.typeCode
                            != nodes[1].dataType.typeCode) {
                        throw Error.error(ErrorCode.X_42562);
                    }

                    if (nodes[2].dataType == null) {
                        throw Error.error(ErrorCode.X_42563);
                    }

                    if (!nodes[2].dataType.isIntervalType()) {
                        throw Error.error(ErrorCode.X_42563);
                    }
                }

                dataType =
                    new ArrayType(nodes[0].getDataType(),
                                  ArrayType.defaultLargeArrayCardinality);

                return;
            }
            case FUNC_DATEADD : {
                int part;

                if (nodes[0].dataType == null) {
                    throw Error.error(ErrorCode.X_42575);
                }

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                part               = getTSIToken((String) nodes[0].valueData);
                nodes[0].valueData = ValuePool.getInt(part);
                nodes[0].dataType  = Type.SQL_INTEGER;
                funcType           = FUNC_TIMESTAMPADD;

                if (nodes[2].dataType == null) {
                    nodes[2].dataType = Type.SQL_TIMESTAMP;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_BIGINT;
                }

                if (!nodes[1].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if (!nodes[2].dataType.isDateOrTimestampType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = nodes[2].dataType;

                return;
            }
            case FUNC_TIMESTAMPADD : {
                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_BIGINT;
                }

                if (!nodes[1].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if (!nodes[2].dataType.isDateOrTimestampType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if (nodes[2].dataType.typeCode == Types.SQL_DATE) {}

                dataType = nodes[2].dataType;

                if (dataType.typeCode == Types.SQL_DATE) {
                    int part = ((Number) nodes[0].valueData).intValue();

                    switch (part) {

                        case Tokens.NANOSECOND :
                        case Tokens.SQL_TSI_FRAC_SECOND :
                        case Tokens.MICROSECOND :
                        case Tokens.MILLISECOND :
                        case Tokens.SQL_TSI_MILLI_SECOND :
                        case Tokens.SECOND :
                        case Tokens.SQL_TSI_SECOND :
                        case Tokens.MINUTE :
                        case Tokens.SQL_TSI_MINUTE :
                        case Tokens.HOUR :
                        case Tokens.SQL_TSI_HOUR :
                            dataType = Type.SQL_TIMESTAMP;
                    }
                }

                return;
            }
            case FUNC_DATEDIFF : {
                int part;

                if (nodes[2] == null) {
                    nodes[2] = nodes[0];
                    nodes[0] = new ExpressionValue(
                        ValuePool.getInt(Tokens.SQL_TSI_DAY),
                        Type.SQL_INTEGER);
                } else {
                    if (!nodes[0].dataType.isCharacterType()) {
                        throw Error.error(ErrorCode.X_42563);
                    }

                    part = getTSIToken((String) nodes[0].valueData);
                    nodes[0].valueData = ValuePool.getInt(part);
                    nodes[0].dataType  = Type.SQL_INTEGER;
                }

                funcType = FUNC_TIMESTAMPDIFF;
            }

            // fall through
            case FUNC_TIMESTAMPDIFF : {
                if (nodes[1].dataType == null) {
                    nodes[1].dataType = nodes[2].dataType;
                }

                if (nodes[2].dataType == null) {
                    nodes[2].dataType = nodes[1].dataType;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_TIMESTAMP;
                    nodes[2].dataType = Type.SQL_TIMESTAMP;
                }

                if (nodes[1].dataType.isCharacterType()) {
                    nodes[1] = new ExpressionOp(
                        nodes[1], Type.SQL_TIMESTAMP_WITH_TIME_ZONE_MAX);
                }

                if (nodes[2].dataType.isCharacterType()) {
                    nodes[2] = new ExpressionOp(
                        nodes[2], Type.SQL_TIMESTAMP_WITH_TIME_ZONE_MAX);
                }

                switch (nodes[1].dataType.typeCode) {

                    case Types.SQL_DATE :
                        if (!nodes[2].dataType.isDateOrTimestampType()) {
                            throw Error.error(ErrorCode.X_42563);
                        }

                        if (nodes[2].dataType.isDateTimeTypeWithZone()) {
                            nodes[1] = new ExpressionOp(
                                nodes[1],
                                Type.SQL_TIMESTAMP_WITH_TIME_ZONE_MAX);
                        }

                        switch (((Integer) nodes[0].valueData).intValue()) {

                            case Tokens.SQL_TSI_DAY :
                            case Tokens.SQL_TSI_WEEK :
                            case Tokens.SQL_TSI_MONTH :
                            case Tokens.SQL_TSI_QUARTER :
                            case Tokens.SQL_TSI_YEAR :
                                break;

                            default :
                                throw Error.error(ErrorCode.X_42563);
                        }
                        break;

                    case Types.SQL_TIMESTAMP :
                        if (!nodes[2].dataType.isDateOrTimestampType()) {
                            throw Error.error(ErrorCode.X_42563);
                        }

                        if (nodes[2].dataType.isDateTimeTypeWithZone()) {
                            nodes[1] = new ExpressionOp(
                                nodes[1],
                                Type.SQL_TIMESTAMP_WITH_TIME_ZONE_MAX);
                        }
                        break;

                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                        if (!nodes[2].dataType.isDateOrTimestampType()) {
                            throw Error.error(ErrorCode.X_42563);
                        }

                        if (!nodes[2].dataType.isDateTimeTypeWithZone()) {
                            nodes[2] = new ExpressionOp(
                                nodes[2],
                                Type.SQL_TIMESTAMP_WITH_TIME_ZONE_MAX);
                        }
                        break;

                    default :
                        throw Error.error(ErrorCode.X_42563);
                }

                dataType = Type.SQL_BIGINT;

                return;
            }
            case FUNC_DATEPART : {

                // only resolve once
                if (extractSpec != 0) {
                    return;
                }

                int part;

                // nodes[0].dataType is constant VARCHAR
                part        = getTSIToken((String) nodes[0].valueData);
                extractSpec = getExtractTokenForTSIPart(part);
                nodes[0] = new ExpressionValue(extractSpec, Type.SQL_INTEGER);
                funcType    = FUNC_EXTRACT;

                super.resolveTypes(session, parent);

                return;
            }
            case FUNC_DATENAME : {

                // only resolve once
                if (extractSpec != 0) {
                    return;
                }

                int part;

                if (nodes[1].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (!nodes[1].dataType.isDateTimeType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                // nodes[0].dataType is constant VARCHAR
                part        = getTSIToken((String) nodes[0].valueData);
                extractSpec = getExtractTokenForTSIPart(part);

                if (extractSpec == Tokens.DAY_OF_WEEK) {
                    extractSpec = Tokens.DAY_NAME;
                } else if (extractSpec == Tokens.MONTH) {
                    extractSpec = Tokens.MONTH_NAME;
                }

                nodes[0] = new ExpressionValue(extractSpec, Type.SQL_INTEGER);
                dataType = Type.SQL_VARCHAR_DEFAULT;

                return;
            }
            case FUNC_DATE_ADD :
            case FUNC_DATE_SUB : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_DATE;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_INTEGER;
                }

                if (nodes[Expression.LEFT].dataType.isCharacterType()) {
                    nodes[Expression.LEFT] =
                        new ExpressionOp(nodes[Expression.LEFT],
                                         Type.SQL_TIMESTAMP);
                }

                if (nodes[Expression.RIGHT].dataType.isIntegralType()) {
                    nodes[Expression.RIGHT] =
                        new ExpressionOp(nodes[Expression.RIGHT],
                                         Type.SQL_INTERVAL_DAY);
                }

                nodes[0].resolveTypes(session, this);
                nodes[1].resolveTypes(session, this);

                dataType = nodes[0].dataType;

                return;
            }
            case FUNC_DAYS : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_DATE;
                }

                switch (nodes[0].dataType.typeCode) {

                    case Types.SQL_DATE :
                    case Types.SQL_TIMESTAMP :
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                        break;

                    default :
                        throw Error.error(ErrorCode.X_42563);
                }

                dataType = Type.SQL_INTEGER;

                return;
            }
            case FUNC_ROUND :
            case FUNC_TRUNC : {
                boolean single = nodes.length == 1 || nodes[1] == null;

                if (nodes[0].dataType == null) {
                    if (single) {
                        if (parent instanceof ExpressionLogical
                                || parent instanceof ExpressionArithmetic) {
                            for (int i = 0; i < parent.nodes.length; i++) {
                                if (parent.nodes[i].dataType != null) {
                                    nodes[0].dataType =
                                        parent.nodes[i].dataType;

                                    break;
                                }
                            }
                        }

                        if (nodes[0].dataType == null) {
                            nodes[0].dataType = Type.SQL_DECIMAL;
                        }

                        if (nodes[0].dataType.isNumberType()) {
                            nodes[0].dataType = Type.SQL_DECIMAL;
                        }
                    } else {
                        if (nodes[1].dataType == null) {
                            nodes[1].dataType = Type.SQL_INTEGER;
                        }

                        if (nodes[1].dataType.isNumberType()) {
                            nodes[0].dataType = Type.SQL_DECIMAL;
                        } else {
                            nodes[0].dataType = Type.SQL_TIMESTAMP;
                        }
                    }
                }

                if (nodes[0].dataType.isDateTimeType()) {
                    if (!single) {
                        if (!nodes[1].dataType.isCharacterType()) {
                            throw Error.error(ErrorCode.X_42566);
                        }
                    }

                    dataType = nodes[0].dataType;

                    break;
                } else if (nodes[0].dataType.isNumberType()) {

                    //
                } else {
                    throw Error.error(ErrorCode.X_42561);
                }
            }

            // fall through
            case FUNC_TRUNCATE : {
                Number offset = null;

                if (nodes[0].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (!nodes[0].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                if (nodes[1] == null) {
                    nodes[1] = new ExpressionValue(ValuePool.INTEGER_0,
                                                   Type.SQL_INTEGER);
                    offset = ValuePool.INTEGER_0;
                } else {
                    if (nodes[1].dataType == null) {
                        nodes[1].dataType = Type.SQL_INTEGER;
                    } else if (!nodes[1].dataType.isIntegralType()) {
                        throw Error.error(ErrorCode.X_42563);
                    }

                    if (nodes[1].opType == OpTypes.VALUE) {
                        offset = (Number) nodes[1].getValue(session);
                    }
                }

                dataType = nodes[0].dataType;

                if (offset != null) {
                    int scale = offset.intValue();

                    if (scale < 0) {
                        scale = 0;
                    } else if (scale > dataType.scale) {
                        scale = dataType.scale;
                    }

                    if (dataType.typeCode == Types.SQL_DECIMAL
                            || dataType.typeCode == Types.SQL_NUMERIC) {
                        if (scale != dataType.scale) {
                            dataType = new NumberType(dataType.typeCode,
                                                      dataType.precision
                                                      - dataType.scale
                                                      + scale, scale);
                        }
                    }
                }

                return;
            }
            case FUNC_TO_CHAR : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_TIMESTAMP;
                }

                if (nodes[1] != null) {
                    if (nodes[1].dataType == null) {
                        nodes[1].dataType = Type.SQL_VARCHAR_DEFAULT;
                    }

                    if (!nodes[1].dataType.isCharacterType()) {
                        throw Error.error(ErrorCode.X_42563);
                    }

                    if (!nodes[0].dataType.isDateTimeType()) {
                        throw Error.error(ErrorCode.X_42563);
                    }
                }

                // fixed maximum as format is a variable
                dataType = CharacterType.getCharacterType(Types.SQL_VARCHAR,
                        64);

                return;
            }
            case FUNC_TO_NUMBER : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR_DEFAULT;
                }

                /*
                                if (nodes[1] != null) {
                                    if (nodes[1].dataType == null) {
                 nodes[1].dataType = Type.SQL_VARCHAR_DEFAULT;
                                    }

                                    if (!nodes[1].dataType.isCharacterType()) {
                                        throw Error.error(ErrorCode.X_42563);
                                    }
                                }
                 */
                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                // fixed maximum as format is a variable
                dataType = Type.SQL_DECIMAL_DEFAULT;

                return;
            }
            case FUNC_TO_DATE :
            case FUNC_TO_TIMESTAMP : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR_DEFAULT;
                }

                if (nodes[1] == null) {
                    String format = "DD-MON-YYYY HH24:MI:SS";

                    if (funcType == FUNC_TO_TIMESTAMP) {
                        format = "DD-MON-YYYY HH24:MI:SS.FF";
                    }

                    nodes[1] = new ExpressionValue(format, Type.SQL_VARCHAR);
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_VARCHAR_DEFAULT;
                }

                if (!nodes[0].dataType.isCharacterType()
                        && !nodes[0].dataType.isDateOrTimestampType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                if (!nodes[1].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                dataType = funcType == FUNC_TO_DATE
                           ? Type.SQL_TIMESTAMP_NO_FRACTION
                           : Type.SQL_TIMESTAMP;

                return;
            }
            case FUNC_TIMESTAMP : {
                Type argType = nodes[0].dataType;

                if (nodes[1] == null) {
                    if (argType == null) {
                        argType = nodes[0].dataType = Type.SQL_VARCHAR_DEFAULT;
                    }

                    if (argType.isCharacterType()
                            || argType.typeCode == Types.SQL_TIMESTAMP
                            || argType.typeCode
                               == Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {}
                    else if (argType.isNumberType()) {}
                    else {
                        throw Error.error(ErrorCode.X_42561);
                    }
                } else {
                    if (argType == null) {
                        if (nodes[1].dataType == null) {
                            argType = nodes[0].dataType = nodes[1].dataType =
                                Type.SQL_VARCHAR_DEFAULT;
                        } else {
                            if (nodes[1].dataType.isCharacterType()) {
                                argType = nodes[0].dataType =
                                    Type.SQL_VARCHAR_DEFAULT;
                            } else {
                                argType = nodes[0].dataType = Type.SQL_DATE;
                            }
                        }
                    }

                    if (nodes[1].dataType == null) {
                        if (argType.isCharacterType()) {
                            nodes[1].dataType = Type.SQL_VARCHAR_DEFAULT;
                        } else if (argType.typeCode == Types.SQL_DATE) {
                            nodes[1].dataType = Type.SQL_TIME_MAX;
                        }
                    }

                    if ((argType.typeCode == Types.SQL_DATE && nodes[1]
                            .dataType.typeCode == Types.SQL_TIME) || argType
                                .isCharacterType() && nodes[1].dataType
                                .isCharacterType()) {}
                    else {
                        throw Error.error(ErrorCode.X_42561);
                    }
                }

                dataType = Type.SQL_TIMESTAMP;

                return;
            }
            case FUNC_TIMESTAMP_WITH_ZONE : {
                Type argType = nodes[0].dataType;

                if (argType == null) {
                    argType = nodes[0].dataType = Type.SQL_BIGINT;
                }

                if (argType.typeCode == Types.SQL_TIMESTAMP
                        || argType.typeCode
                           == Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {}
                else if (argType.isNumberType()) {}
                else {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_TIMESTAMP_WITH_TIME_ZONE;

                return;
            }
            case FUNC_PI :
                dataType = Type.SQL_DOUBLE;
                break;

            case FUNC_SYS_GUID : {
                dataType = Type.SQL_BINARY_16;

                break;
            }
            case FUNC_UUID : {
                if (nodes[0] == null) {
                    dataType = Type.SQL_GUID;
                } else {
                    if (nodes[0].dataType == null) {
                        nodes[0].dataType = Type.SQL_VARCHAR;
                        dataType          = Type.SQL_GUID;
                    } else if (nodes[0].dataType.isCharacterType()) {
                        dataType = Type.SQL_GUID;
                    } else if (nodes[0].dataType.isBinaryType()
                               && !nodes[0].dataType.isLobType()) {
                        dataType = Type.SQL_CHAR_UUID;
                    } else {
                        throw Error.error(ErrorCode.X_42563);
                    }
                }

                break;
            }
            case FUNC_UNIX_MILLIS :
            case FUNC_UNIX_TIMESTAMP : {
                if (nodes[0] != null) {
                    if (nodes[0].dataType == null) {
                        nodes[0].dataType = Type.SQL_TIMESTAMP;
                    } else if (!nodes[0].dataType.isDateTimeType()
                               || nodes[0].dataType.typeCode == Types.SQL_TIME
                               || nodes[0].dataType.typeCode
                                  == Types.SQL_TIME_WITH_TIME_ZONE) {
                        throw Error.error(ErrorCode.X_42563);
                    }
                }

                dataType = Type.SQL_BIGINT;

                break;
            }
            case FUNC_RAND : {
                if (nodes[0] != null) {
                    if (nodes[0].dataType == null) {
                        nodes[0].dataType = Type.SQL_BIGINT;
                    } else if (!nodes[0].dataType.isExactNumberType()) {
                        throw Error.error(ErrorCode.X_42563);
                    }
                }

                dataType = Type.SQL_DOUBLE;

                break;
            }
            case FUNC_ACOS :
            case FUNC_ASIN :
            case FUNC_ATAN :
            case FUNC_COS :
            case FUNC_COSH :
            case FUNC_COT :
            case FUNC_DEGREES :
            case FUNC_SIN :
            case FUNC_SINH :
            case FUNC_TAN :
            case FUNC_TANH :
            case FUNC_LOG10 :
            case FUNC_RADIANS :
            case FUNC_ROUNDMAGIC : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_DOUBLE;
                }

                if (!nodes[0].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_DOUBLE;

                break;
            }
            case FUNC_SIGN : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_DOUBLE;
                }

                if (!nodes[0].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_INTEGER;

                break;
            }
            case FUNC_ATAN2 : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_DOUBLE;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_DOUBLE;
                }

                if (!nodes[0].dataType.isNumberType()
                        || !nodes[1].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_DOUBLE;

                break;
            }
            case FUNC_SOUNDEX : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = CharacterType.getCharacterType(Types.SQL_VARCHAR,
                        4);

                break;
            }
            case FUNC_BITAND :
            case FUNC_BITANDNOT :
            case FUNC_BITNOT :
            case FUNC_BITOR :
            case FUNC_BITXOR : {
                if (funcType == FUNC_BITNOT) {
                    if (nodes[0].dataType == null) {
                        nodes[0].dataType = Type.SQL_INTEGER;
                    }

                    dataType = nodes[0].dataType;
                } else {
                    if (nodes[0].dataType == null) {
                        nodes[0].dataType = nodes[1].dataType;
                    }

                    dataType = nodes[0].dataType;

                    if (nodes[1].dataType == null) {
                        nodes[1].dataType = nodes[0].dataType;
                    }

                    for (int i = 0; i < nodes.length; i++) {
                        if (nodes[i].dataType == null) {
                            nodes[i].dataType = Type.SQL_INTEGER;
                        }
                    }

                    dataType =
                        nodes[0].dataType.getAggregateType(nodes[1].dataType);
                }

                switch (dataType.typeCode) {

                    case Types.SQL_DOUBLE :
                    case Types.SQL_DECIMAL :
                    case Types.SQL_NUMERIC :
                    case Types.SQL_BIGINT :
                    case Types.SQL_INTEGER :
                    case Types.SQL_SMALLINT :
                    case Types.TINYINT :
                        break;

                    case Types.SQL_BIT :
                    case Types.SQL_BIT_VARYING :
                        break;

                    default :
                        throw Error.error(ErrorCode.X_42561);
                }

                break;
            }
            case FUNC_ASCII : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_INTEGER;

                break;
            }
            case FUNC_ASCIISTR : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_VARCHAR_DEFAULT;

                break;
            }
            case FUNC_CHAR : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_INTEGER;
                }

                if (!nodes[0].dataType.isExactNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = CharacterType.getCharacterType(Types.SQL_VARCHAR,
                        1);

                break;
            }
            case FUNC_DIFFERENCE : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_VARCHAR;
                }

                dataType = Type.SQL_INTEGER;

                break;
            }
            case FUNC_FROM_BASE64 : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = nodes[0].dataType.precision == 0
                           ? Type.SQL_VARBINARY_DEFAULT
                           : BinaryType.getBinaryType(
                               Types.SQL_VARBINARY,
                               (nodes[0].dataType.precision * 3) / 4);

                break;
            }
            case FUNC_UNISTR : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR_DEFAULT;
                }

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = nodes[0].dataType;

                break;
            }
            case FUNC_UNHEX :
            case FUNC_HEXTORAW : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = nodes[0].dataType.precision == 0
                           ? Type.SQL_VARBINARY_DEFAULT
                           : BinaryType.getBinaryType(
                               Types.SQL_VARBINARY,
                               nodes[0].dataType.precision / 2);

                break;
            }
            case FUNC_TO_BASE64 : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARBINARY;
                }

                if (!nodes[0].dataType.isBinaryType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if (nodes[0].dataType.precision == 0) {
                    dataType = Type.SQL_VARCHAR_DEFAULT;
                } else {
                    long precision = ((nodes[0].dataType.precision + 2) / 3)
                                     * 4;

                    dataType =
                        CharacterType.getCharacterType(Types.SQL_VARCHAR,
                                                       precision);
                }

                break;
            }
            case FUNC_HEX : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARBINARY;
                }

                if (nodes[0].dataType.isNumberType()) {
                    dataType = Type.SQL_VARCHAR_DEFAULT;

                    break;
                }

                dataType = nodes[0].dataType.precision == 0
                           ? Type.SQL_VARCHAR_DEFAULT
                           : CharacterType.getCharacterType(Types.SQL_VARCHAR,
                           nodes[0].dataType.precision * 2);

                break;
            }
            case FUNC_RAWTOHEX : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARBINARY;
                }

                if (!nodes[0].dataType.isBinaryType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = nodes[0].dataType.precision == 0
                           ? Type.SQL_VARCHAR_DEFAULT
                           : CharacterType.getCharacterType(Types.SQL_VARCHAR,
                           nodes[0].dataType.precision * 2);

                break;
            }
            case FUNC_REPEAT : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                boolean isChar = nodes[0].dataType.isCharacterType();

                if (!isChar && !nodes[0].dataType.isBinaryType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if (!nodes[1].dataType.isExactNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = isChar ? Type.SQL_VARCHAR_DEFAULT
                                  : Type.SQL_VARBINARY_DEFAULT;

                break;
            }
            case FUNC_REPLACE : {
                if (nodes[2] == null) {
                    nodes[2] = new ExpressionValue("", Type.SQL_VARCHAR);
                }

                for (int i = 0; i < nodes.length; i++) {
                    if (nodes[i].dataType == null) {
                        nodes[i].dataType = Type.SQL_VARCHAR;
                    } else if (!nodes[i].dataType.isCharacterType()) {
                        throw Error.error(ErrorCode.X_42561);
                    }
                }

                dataType = Type.SQL_VARCHAR_DEFAULT;

                break;
            }
            case FUNC_LEFT :
            case FUNC_RIGHT : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_INTEGER;
                }

                if (!nodes[1].dataType.isExactNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = nodes[0].dataType.precision == 0
                           ? Type.SQL_VARCHAR_DEFAULT
                           : ((CharacterType) nodes[0].dataType)
                               .getCharacterType(nodes[0].dataType.precision);

                break;
            }
            case FUNC_SPACE : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_INTEGER;
                }

                if (!nodes[0].dataType.isIntegralType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_VARCHAR_DEFAULT;

                break;
            }
            case FUNC_REVERSE : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR_DEFAULT;
                }

                dataType = nodes[0].dataType;

                if (!dataType.isCharacterType() || dataType.isLobType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                break;
            }
            case FUNC_REGEXP_REPLACE :
            case FUNC_REGEXP_COUNT :
            case FUNC_REGEXP_INSTR :
            case FUNC_REGEXP_LIKE :
            case FUNC_REGEXP_SUBSTR :
            case FUNC_REGEXP_MATCHES :
            case FUNC_REGEXP_SUBSTRING :
            case FUNC_REGEXP_SUBSTRING_ARRAY : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR_DEFAULT;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_VARCHAR_DEFAULT;
                }

                if (!nodes[0].dataType.isCharacterType()
                        || !nodes[1].dataType.isCharacterType()
                        || nodes[1].dataType.isLobType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if (nodes[1].exprSubType == OpTypes.VALUE) {
                    String matchPattern = (String) nodes[1].getValue(session);

                    pattern = Pattern.compile(matchPattern);
                }

                switch (funcType) {

                    case FUNC_REGEXP_COUNT :
                    case FUNC_REGEXP_INSTR :
                        dataType = Type.SQL_INTEGER;
                        break;

                    case FUNC_REGEXP_LIKE :
                    case FUNC_REGEXP_MATCHES :
                        dataType = Type.SQL_BOOLEAN;
                        break;

                    case FUNC_REGEXP_REPLACE :
                        dataType = Type.SQL_VARCHAR_DEFAULT;
                        break;

                    case FUNC_REGEXP_SUBSTR :
                    case FUNC_REGEXP_SUBSTRING :
                        dataType = Type.SQL_VARCHAR_DEFAULT;
                        break;

                    case FUNC_REGEXP_SUBSTRING_ARRAY :
                        dataType = Type.getDefaultArrayType(Types.SQL_VARCHAR);
                        break;

                    default :
                }

                break;
            }
            case FUNC_CRYPT_KEY : {
                for (int i = 0; i < nodes.length; i++) {
                    if (nodes[i].dataType == null) {
                        nodes[i].dataType = Type.SQL_VARCHAR;
                    } else if (!nodes[i].dataType.isCharacterType()) {
                        throw Error.error(ErrorCode.X_42561);
                    }
                }

                dataType = Type.SQL_VARCHAR_DEFAULT;

                break;
            }
            case FUNC_LOAD_FILE : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR_DEFAULT;
                }

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if (nodes[1] == null) {
                    dataType = Type.SQL_BLOB;
                } else {
                    dataType = Type.SQL_CLOB;

                    if (nodes[1].dataType == null
                            || !nodes[1].dataType.isCharacterType()) {
                        throw Error.error(ErrorCode.X_42561);
                    }
                }

                break;
            }
            case FUNC_LPAD :
            case FUNC_RPAD : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR_DEFAULT;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_INTEGER;
                }

                if (!nodes[1].dataType.isIntegralType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if (nodes[2] != null) {
                    if (nodes[2].dataType == null) {
                        nodes[2].dataType = Type.SQL_VARCHAR_DEFAULT;
                    }

                    if (!nodes[2].dataType.isCharacterType()) {
                        throw Error.error(ErrorCode.X_42561);
                    }
                }

                dataType = nodes[0].dataType;

                if (dataType.typeCode != Types.SQL_CLOB) {
                    dataType = Type.SQL_VARCHAR_DEFAULT;
                }

                if (nodes[1].opType == OpTypes.VALUE) {
                    Number value = (Number) nodes[1].getValue(session);

                    if (value != null) {
                        dataType = ((CharacterType) dataType).getCharacterType(
                            value.longValue());
                    }
                }

                break;
            }
            case FUNC_POSITION_ARRAY : {
                if (nodes[1].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (!nodes[1].dataType.isArrayType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                if (nodes[0].dataType == null) {
                    nodes[0].dataType = nodes[1].dataType.collectionBaseType();
                }

                if (!nodes[1].dataType.collectionBaseType().canCompareDirect(
                        nodes[0].dataType)) {
                    throw Error.error(ErrorCode.X_42563);
                }

                if (nodes[2] != null) {
                    if (nodes[2].dataType == null) {
                        nodes[2].dataType = Type.SQL_INTEGER;
                    }

                    if (!nodes[2].dataType.isIntegralType()) {
                        throw Error.error(ErrorCode.X_42563);
                    }
                }

                dataType = Type.SQL_INTEGER;

                break;
            }
            case FUNC_SORT_ARRAY : {
                if (nodes[0].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (!nodes[0].dataType.isArrayType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                if (nodes[1] == null) {
                    nodes[1] =
                        new ExpressionValue(ValuePool.getInt(Tokens.ASC),
                                            Type.SQL_INTEGER);
                }

                if (nodes[2] == null) {
                    nodes[2] =
                        new ExpressionValue(ValuePool.getInt(Tokens.FIRST),
                                            Type.SQL_INTEGER);
                }

                dataType = nodes[0].dataType;

                break;
            }
            case FUNC_ADD_MONTHS :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_TIMESTAMP_NO_FRACTION;
                }

                if (!nodes[0].dataType.isDateOrTimestampType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                dataType = Type.SQL_TIMESTAMP_NO_FRACTION;
                break;

            case FUNC_DBTIMEZONE :
                dataType = CharacterType.getCharacterType(Types.SQL_VARCHAR,
                        6);
                break;

            case FUNC_FROM_TZ :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_TIMESTAMP;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isTimestampType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                if (!nodes[1].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                dataType = Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
                break;

            case FUNC_LAST_DAY :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_TIMESTAMP_NO_FRACTION;
                }

                if (nodes[0].dataType.isCharacterType()) {
                    nodes[0] =
                        new ExpressionOp(nodes[0],
                                         Type.SQL_TIMESTAMP_NO_FRACTION);
                }

                if (!nodes[0].dataType.isDateOrTimestampType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                dataType = Type.SQL_TIMESTAMP_NO_FRACTION;
                break;

            case FUNC_MONTHS_BETWEEN :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_TIMESTAMP_NO_FRACTION;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_TIMESTAMP_NO_FRACTION;
                }

                if (!nodes[0].dataType.isDateOrTimestampType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                if (!nodes[1].dataType.isDateOrTimestampType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                dataType = Type.SQL_DECIMAL_DEFAULT;
                break;

            case FUNC_NANVL :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_DOUBLE;
                }

                if (nodes[0].dataType.isCharacterType()) {
                    nodes[0] = new ExpressionOp(nodes[0], Type.SQL_DOUBLE);
                }

                dataType = nodes[0].dataType;
                break;

            case FUNC_NEXT_DAY :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_TIMESTAMP_NO_FRACTION;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_VARCHAR;
                }

                dataType = Type.SQL_TIMESTAMP_NO_FRACTION;
                break;

            case FUNC_NUMTODSINTERVAL :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_DOUBLE;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                if (!nodes[1].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                dataType = Type.SQL_INTERVAL_DAY_TO_SECOND_MAX_PRECISION;
                break;

            case FUNC_NUMTOYMINTERVAL :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_DOUBLE;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                if (!nodes[1].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                dataType = Type.SQL_INTERVAL_YEAR_TO_MONTH_MAX_PRECISION;
                break;

            case FUNC_SESSIONTIMEZONE :
                dataType = CharacterType.getCharacterType(Types.SQL_VARCHAR,
                        6);
                break;

            case FUNC_SYS_EXTRACT_UTC :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
                }

                dataType = Type.SQL_TIMESTAMP;
                break;

            case FUNC_SYSDATE :
                dataType = Type.SQL_TIMESTAMP_NO_FRACTION;
                break;

            case FUNC_SYSTIMESTAMP :
                dataType = Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
                break;

            case FUNC_TO_DSINTERVAL :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                dataType = Type.SQL_INTERVAL_DAY_TO_SECOND_MAX_PRECISION;
                break;

            case FUNC_TO_YMINTERVAL :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                dataType = Type.SQL_INTERVAL_YEAR_TO_MONTH_MAX_PRECISION;
                break;

            case FUNC_TO_TIMESTAMP_TZ :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR_DEFAULT;
                }

                if (nodes[1] == null) {
                    String format = "DD-MON-YYYY HH24:MI:SS:FF TZH:TZM";

                    nodes[1] = new ExpressionValue(format, Type.SQL_VARCHAR);
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isCharacterType()
                        || !nodes[1].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42567);
                }

                dataType = Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
                break;

            case FUNC_TRANSACTION_UTC :
                dataType = Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
                break;

            case FUNC_TRANSLATE :
                for (int i = 0; i < nodes.length; i++) {
                    if (nodes[i].dataType == null) {
                        nodes[i].dataType = Type.SQL_VARCHAR_DEFAULT;
                    }

                    if (!nodes[i].dataType.isCharacterType()
                            || nodes[i].dataType.isLobType()) {
                        throw Error.error(ErrorCode.X_42563);
                    }
                }

                if (nodes[1].valueData != null && nodes[2].valueData != null) {
                    this.charLookup =
                        getTranslationMap((String) nodes[1].valueData,
                                          (String) nodes[2].valueData);
                }

                dataType = nodes[0].dataType;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionCustom");
        }
    }

    public String getSQL() {

        switch (funcType) {

            case FUNC_POSITION_CHAR : {

                // LOCATE
                StringBuilder sb = new StringBuilder(Tokens.T_LOCATE).append(
                    Tokens.T_OPENBRACKET).append(nodes[0].getSQL()).append(
                    Tokens.T_COMMA).append(nodes[1].getSQL());

                if (nodes.length > 3 && nodes[3] != null) {
                    sb.append(Tokens.T_COMMA).append(nodes[3].getSQL());
                }

                sb.append(Tokens.T_CLOSEBRACKET).toString();

                return sb.toString();
            }
            case FUNC_LPAD :
            case FUNC_RPAD : {
                StringBuilder sb = new StringBuilder(name);

                sb.append(Tokens.T_OPENBRACKET).append(nodes[0].getSQL());
                sb.append(Tokens.T_COMMA).append(nodes[1].getSQL());

                if (nodes[2] != null) {
                    sb.append(Tokens.T_COMMA).append(nodes[2].getSQL());
                }

                sb.append(Tokens.T_CLOSEBRACKET).toString();

                return sb.toString();
            }
            case FUNC_EXTRACT :
            case FUNC_TRIM_CHAR :
            case FUNC_OVERLAY_CHAR :
                return super.getSQL();

            case FUNC_POSITION_ARRAY : {
                StringBuilder sb = new StringBuilder(name).append('(');

                sb.append(nodes[0].getSQL()).append(' ').append(Tokens.T_IN);
                sb.append(' ').append(nodes[1].getSQL());

                if (nodes[2] != null) {
                    sb.append(' ').append(Tokens.T_FROM);
                    sb.append(' ').append(nodes[2].getSQL());
                }

                sb.append(')');

                return sb.toString();
            }
            case FUNC_SORT_ARRAY : {
                StringBuilder sb = new StringBuilder(name).append('(');

                sb.append(nodes[0].getSQL());

                if (((Number) nodes[1].valueData).intValue() == Tokens.DESC) {
                    sb.append(' ').append(Tokens.T_DESC);
                }

                if (((Number) nodes[2].valueData).intValue() == Tokens.LAST) {
                    sb.append(' ').append(Tokens.T_NULLS).append(' ');
                    sb.append(Tokens.T_LAST);
                }

                sb.append(')');

                return sb.toString();
            }
            case FUNC_SYSDATE :
            case FUNC_SYSTIMESTAMP :
                return name;

            case FUNC_DATABASE :
            case FUNC_DATABASE_NAME :
            case FUNC_ISAUTOCOMMIT :
            case FUNC_ISREADONLYSESSION :
            case FUNC_ISREADONLYDATABASE :
            case FUNC_ISREADONLYDATABASEFILES :
            case FUNC_ISOLATION_LEVEL :
            case FUNC_SESSION_ISOLATION_LEVEL :
            case FUNC_DATABASE_ISOLATION_LEVEL :
            case FUNC_TRANSACTION_CONTROL :
            case FUNC_TIMEZONE :
            case FUNC_SESSION_TIMEZONE :
            case FUNC_DATABASE_TIMEZONE :
            case FUNC_DATABASE_VERSION :
            case FUNC_PI :
            case FUNC_IDENTITY :
            case FUNC_SESSION_ID :
            case FUNC_ACTION_ID :
            case FUNC_SYS_GUID :
            case FUNC_TRANSACTION_ID :
            case FUNC_TRANSACTION_SIZE :
            case FUNC_TRANSACTION_UTC :
                return new StringBuilder(name).append(
                    Tokens.T_OPENBRACKET).append(
                    Tokens.T_CLOSEBRACKET).toString();

            case FUNC_TIMESTAMPADD : {
                String token = Tokens.getSQLTSIString(
                    ((Number) nodes[0].getValue(null)).intValue());

                return new StringBuilder(Tokens.T_TIMESTAMPADD).append(
                    Tokens.T_OPENBRACKET).append(token).append(
                    Tokens.T_COMMA).append(nodes[1].getSQL()).append(
                    Tokens.T_COMMA).append(nodes[2].getSQL()).append(
                    Tokens.T_CLOSEBRACKET).toString();
            }
            case FUNC_TIMESTAMPDIFF : {
                String token = Tokens.getSQLTSIString(
                    ((Number) nodes[0].getValue(null)).intValue());

                return new StringBuilder(Tokens.T_TIMESTAMPDIFF).append(
                    Tokens.T_OPENBRACKET).append(token).append(
                    Tokens.T_COMMA).append(nodes[1].getSQL()).append(
                    Tokens.T_COMMA).append(nodes[2].getSQL()).append(
                    Tokens.T_CLOSEBRACKET).toString();
            }
            case FUNC_DATE_ADD :
                return new StringBuilder(nodes[0].getSQL()).append(' ').append(
                    '+').append(nodes[1].getSQL()).toString();

            case FUNC_DATE_SUB :
                return new StringBuilder(nodes[0].getSQL()).append(' ').append(
                    '-').append(nodes[1].getSQL()).toString();

            case FUNC_UNIX_MILLIS :
            case FUNC_UNIX_TIMESTAMP :
            case FUNC_UUID :
            case FUNC_RAND :
            case FUNC_LOAD_FILE :
            case FUNC_ROUND :
            case FUNC_TIMESTAMP :
            case FUNC_TO_DATE :
            case FUNC_TO_NUMBER :
            case FUNC_TO_TIMESTAMP :
            case FUNC_TO_TIMESTAMP_TZ :
            case FUNC_TRUNC :
            case FUNC_TRUNCATE :
            case FUNC_TO_CHAR :
            case FUNC_ACOS :
            case FUNC_ASCII :
            case FUNC_ASCIISTR :
            case FUNC_ASIN :
            case FUNC_ATAN :
            case FUNC_CHAR :
            case FUNC_COS :
            case FUNC_COSH :
            case FUNC_COT :
            case FUNC_DAYS :
            case FUNC_DEGREES :
            case FUNC_SIN :
            case FUNC_SINH :
            case FUNC_TAN :
            case FUNC_TANH :
            case FUNC_LOG10 :
            case FUNC_RADIANS :
            case FUNC_ROUNDMAGIC :
            case FUNC_SIGN :
            case FUNC_SOUNDEX :
            case FUNC_SPACE :
            case FUNC_REVERSE :
            case FUNC_HEX :
            case FUNC_UNISTR :
            case FUNC_UNHEX :
            case FUNC_FROM_BASE64 :
            case FUNC_TO_BASE64 :
            case FUNC_HEXTORAW :
            case FUNC_RAWTOHEX :
            case FUNC_LOB_ID :
            case FUNC_ATAN2 :
            case FUNC_BITAND :
            case FUNC_BITANDNOT :
            case FUNC_BITOR :
            case FUNC_BITNOT :
            case FUNC_BITXOR :
            case FUNC_CRYPT_KEY :
            case FUNC_DIFFERENCE :
            case FUNC_LEFT :
            case FUNC_RIGHT :
            case FUNC_REGEXP_COUNT :
            case FUNC_REGEXP_INSTR :
            case FUNC_REGEXP_LIKE :
            case FUNC_REGEXP_MATCHES :
            case FUNC_REGEXP_SUBSTRING :
            case FUNC_REGEXP_SUBSTRING_ARRAY :
            case FUNC_REPEAT :
            case FUNC_SEQUENCE_ARRAY :
            case FUNC_REGEXP_REPLACE :
            case FUNC_REPLACE :
            case FUNC_ADD_MONTHS :
            case FUNC_DATENAME :
            case FUNC_DATEPART :
            case FUNC_DBTIMEZONE :
            case FUNC_FROM_TZ :
            case FUNC_LAST_DAY :
            case FUNC_MONTHS_BETWEEN :
            case FUNC_NEXT_DAY :
            case FUNC_NANVL :
            case FUNC_NUMTODSINTERVAL :
            case FUNC_NUMTOYMINTERVAL :
            case FUNC_SESSIONTIMEZONE :
            case FUNC_SYS_EXTRACT_UTC :
            case FUNC_TO_DSINTERVAL :
            case FUNC_TO_YMINTERVAL :
            case FUNC_TRANSLATE :
                return getSQLSimple();

            case FUNC_DIAGNOSTICS : {
                StringBuilder sb = new StringBuilder(name).append('(');

                //exprSubType == ExpressionColumn.idx_row_count
                sb.append(Tokens.T_ROW_COUNT);
                sb.append(')');

                return sb.toString();
            }
            default :
                return super.getSQL();
        }
    }

    private String getSQLSimple() {

        StringBuilder sb    = new StringBuilder(name).append('(');
        boolean       added = false;

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                if (added) {
                    sb.append(',');
                }

                sb.append(nodes[i].getSQL());

                added = true;
            }
        }

        sb.append(')');

        return sb.toString();
    }

    /**
     * Returns a four character code representing the sound of the given
     * <code>String</code>. Non-ASCCI characters in the
     * input <code>String</code> are ignored. <p>
     *
     * This method was rewritten for HSQLDB to comply with the description at
     * <a href="http://www.archives.gov/research/census/soundex.html">
     * http://www.archives.gov/research/census/soundex.html </a>.<p>
     * @param s the <code>String</code> for which to calculate the 4 character
     *      <code>SOUNDEX</code> value
     * @return the 4 character <code>SOUNDEX</code> value for the given
     *      <code>String</code>
     */
    private static char[] soundex(String s) {

        if (s == null) {
            return null;
        }

        s = s.toUpperCase(Locale.ENGLISH);

        int    len       = s.length();
        char[] b         = new char[] {
            '0', '0', '0', '0'
        };
        char   lastdigit = '0';

        for (int i = 0, j = 0; i < len && j < 4; i++) {
            char c = s.charAt(i);
            char newdigit;

            if ("AEIOUY".indexOf(c) != -1) {
                newdigit = '7';
            } else if (c == 'H' || c == 'W') {
                newdigit = '8';
            } else if ("BFPV".indexOf(c) != -1) {
                newdigit = '1';
            } else if ("CGJKQSXZ".indexOf(c) != -1) {
                newdigit = '2';
            } else if (c == 'D' || c == 'T') {
                newdigit = '3';
            } else if (c == 'L') {
                newdigit = '4';
            } else if (c == 'M' || c == 'N') {
                newdigit = '5';
            } else if (c == 'R') {
                newdigit = '6';
            } else {
                continue;
            }

            if (j == 0) {
                b[j++]    = c;
                lastdigit = newdigit;
            } else if (newdigit <= '6') {
                if (newdigit != lastdigit) {
                    b[j++]    = newdigit;
                    lastdigit = newdigit;
                }
            } else if (newdigit == '7') {
                lastdigit = newdigit;
            }
        }

        return b;
    }

    private static int getTSIToken(String string) {

        int part;

        if ("yy".equalsIgnoreCase(string) || "yyyy".equalsIgnoreCase(string)
                || "year".equalsIgnoreCase(string)) {
            part = Tokens.SQL_TSI_YEAR;
        } else if ("qq".equalsIgnoreCase(string)
                   || "quarter".equalsIgnoreCase(string)) {
            part = Tokens.SQL_TSI_QUARTER;
        } else if ("mm".equalsIgnoreCase(string)
                   || "month".equalsIgnoreCase(string)) {
            part = Tokens.SQL_TSI_MONTH;
        } else if ("dd".equalsIgnoreCase(string)
                   || "day".equalsIgnoreCase(string)) {
            part = Tokens.SQL_TSI_DAY;
        } else if ("dy".equalsIgnoreCase(string)
                   || "dayofyear".equalsIgnoreCase(string)) {
            part = Tokens.DAY_OF_YEAR;
        } else if ("dw".equalsIgnoreCase(string)
                   || "weekday".equalsIgnoreCase(string)) {
            part = Tokens.DAY_OF_WEEK;
        } else if ("wk".equalsIgnoreCase(string)
                   || "week".equalsIgnoreCase(string)) {
            part = Tokens.SQL_TSI_WEEK;
        } else if ("hh".equalsIgnoreCase(string)
                   || "hour".equalsIgnoreCase(string)) {
            part = Tokens.SQL_TSI_HOUR;
        } else if ("mi".equalsIgnoreCase(string)
                   || "minute".equalsIgnoreCase(string)) {
            part = Tokens.SQL_TSI_MINUTE;
        } else if ("ss".equalsIgnoreCase(string)
                   || "second".equalsIgnoreCase(string)) {
            part = Tokens.SQL_TSI_SECOND;
        } else if ("ms".equalsIgnoreCase(string)
                   || "millisecond".equalsIgnoreCase(string)) {
            part = Tokens.SQL_TSI_MILLI_SECOND;
        } else if ("mcs".equalsIgnoreCase(string)
                   || "microsecond".equalsIgnoreCase(string)) {
            part = Tokens.SQL_TSI_MICRO_SECOND;
        } else if ("ns".equalsIgnoreCase(string)
                   || "nanosecond".equalsIgnoreCase(string)) {
            part = Tokens.SQL_TSI_FRAC_SECOND;
        } else if ("tz".equalsIgnoreCase(string)
                   || "tzoffset".equalsIgnoreCase(string)) {
            part = Tokens.TIMEZONE;
        } else {
            throw Error.error(ErrorCode.X_42566, string);
        }

        return part;
    }

    private static int getExtractTokenForTSIPart(int part) {

        switch (part) {

            case Tokens.SQL_TSI_FRAC_SECOND :
                return Tokens.NANOSECOND;

            case Tokens.SQL_TSI_MILLI_SECOND :
                return Tokens.MILLISECOND;

            case Tokens.SQL_TSI_SECOND :
                return Tokens.SECOND;

            case Tokens.SQL_TSI_MINUTE :
                return Tokens.MINUTE;

            case Tokens.SQL_TSI_HOUR :
                return Tokens.HOUR;

            case Tokens.SQL_TSI_DAY :
                return Tokens.DAY;

            case Tokens.DAY_OF_WEEK :
                return Tokens.DAY_OF_WEEK;

            case Tokens.DAY_OF_YEAR :
                return Tokens.DAY_OF_YEAR;

            case Tokens.TIMEZONE :
                return Tokens.TIMEZONE;

            case Tokens.SQL_TSI_WEEK :
                return Tokens.WEEK_OF_YEAR;

            case Tokens.SQL_TSI_MONTH :
                return Tokens.MONTH;

            case Tokens.SQL_TSI_QUARTER :
                return Tokens.QUARTER;

            case Tokens.SQL_TSI_YEAR :
                return Tokens.YEAR;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionCustom");
        }
    }

    static int getSQLTypeForToken(String string) {

        int type = -1;

        if ("YEAR_MONTH".equalsIgnoreCase(string)) {
            type = Types.SQL_INTERVAL_YEAR_TO_MONTH;
        } else if ("DAY_HOUR".equalsIgnoreCase(string)) {
            type = Types.SQL_INTERVAL_DAY_TO_HOUR;
        } else if ("DAY_MINUTE".equalsIgnoreCase(string)) {
            type = Types.SQL_INTERVAL_DAY_TO_MINUTE;
        } else if ("DAY_SECOND".equalsIgnoreCase(string)) {
            type = Types.SQL_INTERVAL_DAY_TO_SECOND;
        } else if ("DAY_MICROSECOND".equalsIgnoreCase(string)) {
            type = Types.SQL_INTERVAL_DAY_TO_SECOND;
        } else if ("HOUR_MINUTE".equalsIgnoreCase(string)) {
            type = Types.SQL_INTERVAL_HOUR_TO_MINUTE;
        } else if ("HOUR_SECOND".equalsIgnoreCase(string)) {
            type = Types.SQL_INTERVAL_HOUR_TO_SECOND;
        } else if ("HOUR_MICROSECOND".equalsIgnoreCase(string)) {
            type = Types.SQL_INTERVAL_HOUR_TO_SECOND;
        } else if ("MINUTE_SECOND".equalsIgnoreCase(string)) {
            type = Types.SQL_INTERVAL_MINUTE_TO_SECOND;
        } else if ("MINUTE_MICROSECOND".equalsIgnoreCase(string)) {
            type = Types.SQL_INTERVAL_MINUTE_TO_SECOND;
        } else if ("SECOND_MICROSECOND".equalsIgnoreCase(string)) {
            type = Types.SQL_INTERVAL_SECOND;
        }

        return type;
    }

    private static IntKeyIntValueHashMap getTranslationMap(String source,
            String dest) {

        IntKeyIntValueHashMap map = new IntKeyIntValueHashMap();

        for (int i = 0; i < source.length(); i++) {
            int character = source.charAt(i);

            if (i >= dest.length()) {
                map.put(character, -1);

                continue;
            }

            int value = dest.charAt(i);

            map.put(character, value);
        }

        return map;
    }

    private static String translateWithMap(String source,
                                           IntKeyIntValueHashMap map) {

        StringBuilder sb = new StringBuilder(source.length());

        for (int i = 0; i < source.length(); i++) {
            int character = source.charAt(i);
            int value     = map.get(character, -2);

            if (value == -2) {
                sb.append((char) character);
            } else if (value == -1) {

                //
            } else {
                sb.append((char) value);
            }
        }

        return sb.toString();
    }

    private static int regexpParams(String params) {

        int flags = 0;

        if (params == null) {
            return flags;
        }

        for (int i = 0; i < params.length(); ++i) {
            switch (params.charAt(i)) {

                case 'i' :
                    flags |= Pattern.CASE_INSENSITIVE;
                    break;

                case 'c' :
                    flags &= ~Pattern.CASE_INSENSITIVE;
                    flags |= Pattern.UNICODE_CASE;
                    break;

                case 'n' :
                    flags |= Pattern.DOTALL;
                    break;

                case 'm' :
                    flags |= Pattern.MULTILINE;
                    break;

                default :
                    throw Error.error(ErrorCode.X_22511, params);
            }
        }

        return flags;
    }
}
