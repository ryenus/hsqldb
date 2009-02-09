/*
 * $Id$
 */

package org.hsqldb.server;

import org.hsqldb.types.Type;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.BooleanType;
import org.hsqldb.types.CharacterType;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.Session;
import org.hsqldb.Types;
import java.sql.SQLException;
import java.io.Serializable;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.HsqlException;
import org.hsqldb.types.BinaryData;
import org.hsqldb.Error;
import org.hsqldb.ErrorCode;
import org.hsqldb.jdbc.Util;

public class PgType {
    private int oid;
    private int typeWidth = -1;
    private int lpConstraint = -1; // Length or Precision
    private Type hType;

    public int getOid() {
        return oid;
    }
    public int getTypeWidth() {
        return typeWidth;
    }
    public int getLPConstraint() {
        return lpConstraint;
    }

    /**
     * Convenience wrapper for PgType constructor, when there is no
     * type width, length, or precision setting for the type.
     *
     * @see #PgType(Type, int, Integer, Integer)
     */
    protected PgType(Type hType, int oid) {
        this(hType, oid, null, null);
    }

    /**
     * Convenience wrapper for PgType constructor, when there is no
     * length or precision setting for the type.
     *
     * @see #PgType(Type, int, Integer, Integer)
     */
    protected PgType(Type hType, int oid, int typeWidth) {
        this(hType, oid, new Integer(typeWidth), null);
    }

    /**
     * Convenience wrapper for PgType constructor, when there is no fixed
     * width for the type.
     *
     * @param dummy Normally pass null.  This is a dummy parameter just to make
     *              a unique method signature.  If non-null, will be treated
     *              exactly the same as the typeWidthObject from the 3-param
     *              constructor.
     * @see #PgType(hType, int, Integer, Integer)
     */
    protected PgType(Type hType, int oid, Integer dummy, long lpConstraint) {
        this(hType, oid, dummy, new Integer((int) lpConstraint));
        if (lpConstraint < 0) {
            throw new IllegalArgumentException(
                "Length/Precision value is below minimum value of 0");
        }
        if (lpConstraint > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "Length/Precision value is above maximum value of "
                + Integer.MAX_VALUE);
        }
    }

    /**
     * @param hType HyperSQL data type
     * @param oid Numeric Object ID for the driver-side type.
     * @param typeWidth Fixed width for the type
     * @param lpConstraint Either length or Precision setting for this
     *                     instance of the type
     */
    protected PgType(Type hType,
        int oid, Integer typeWidthObject, Integer lpConstraintObject) {
        this.hType = hType;
        this.oid = oid;
        this.typeWidth = (typeWidthObject == null)
                       ? -1 : typeWidthObject.intValue();
        this.lpConstraint = (lpConstraintObject == null)
                       ? -1 : lpConstraintObject.intValue();
    }

    static public PgType getPgType(Type hType) {
        switch (hType.typeCode) {
            case Types.TINYINT:
                throw new IllegalArgumentException(
                    "Driver doesn't support type 'TINYINT' yet");
            case Types.SQL_SMALLINT:
                return int2singleton;
            case Types.SQL_INTEGER:
                return int4singleton;
            case Types.SQL_BIGINT:
                return int8singleton;

            case Types.SQL_NUMERIC:
            case Types.SQL_DECIMAL:
                return new PgType(hType, TYPE_NUMERIC, null,
                        (hType.precision << 16) + hType.scale);

            case Types.SQL_FLOAT:
                // TODO:
                // Improve the driver to make use of the Float precision
                // return new PgType(hType, TYPE_FLOAT8, null, hType.precision);
            case Types.SQL_DOUBLE:
            case Types.SQL_REAL:
                return doubleSingleton;

            case Types.BOOLEAN:
                return boolSingleton;

            case Types.SQL_CHAR: // = CHARACTER
                if (hType.getNameString() == null) { // Non-direct-table-col
                    return unknownSingleton;  // constant value
                }
                return new PgType(hType, TYPE_BPCHAR, null, hType.precision);

            case Types.SQL_VARCHAR: // = CHARACTER VARYING = LONGVARCHAR
            case Types.VARCHAR_IGNORECASE: // Don't know if possible here
                if (hType.precision < 0) {
                    throw new IllegalArgumentException(
                        "Length/Precision value is below minimum value of 0");
                }
                if (hType.precision > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException(
                        "Length/Precision value is above maximum value of "
                        + Integer.MAX_VALUE);
                }
                return (hType.precision == 0 || hType.getNameString() == null)
                    ? textSingleton
                    : new PgType(hType, TYPE_VARCHAR, null, hType.precision);
                // Return TEXT type for both unlimited VARCHARs, and for
                // Non-direct-table-col results.
            case Types.SQL_CLOB: // = CHARACTER LARGE OBJECT
                throw new IllegalArgumentException(
                    "Driver doesn't support type 'CLOB' yet");

            case Types.SQL_BINARY:
                throw new IllegalArgumentException(
                    "Driver doesn't support type 'BINARY' yet");
            case Types.SQL_BLOB: // = BINARY LARGE OBJECT
                throw new IllegalArgumentException(
                    "Driver doesn't support type 'BLOB' yet");
            case Types.SQL_VARBINARY: // = BINARY VARYING
                return new PgType(hType, TYPE_BYTEA, null, hType.precision);

            case Types.OTHER:
                throw new IllegalArgumentException(
                    "Driver doesn't support type 'OTHER' yet");

            case Types.SQL_BIT:
                return bitSingleton;
            case Types.SQL_BIT_VARYING:
                return bitVaryingSingleton;
                // I have no idea why length contstaint spec is not needed for
                // BIT_VARYING.

            case Types.SQL_DATE:
                return dateSingleton;
                // 4 bytes
            case Types.SQL_TIME:
                return new PgType(hType, TYPE_TIME, 8, hType.precision);
            case Types.SQL_TIME_WITH_TIME_ZONE:
                return new PgType(
                    hType, TYPE_TIME_WITH_TMZONE, 12, hType.precision);
            case Types.SQL_TIMESTAMP:
                return new PgType(
                    hType, TYPE_TIMESTAMP_NO_TMZONE, 8, hType.precision);
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE:
                return new PgType(hType, TYPE_TIMESTAMP, 8, hType.precision);
                // Postgresql is returning type DATETIME for this case.
                // It should return TYPE_TIMESTAMP, no?

            case Types.SQL_INTERVAL:
            case Types.SQL_INTERVAL_YEAR:
            case Types.SQL_INTERVAL_YEAR_TO_MONTH:
            case Types.SQL_INTERVAL_MONTH:
            case Types.SQL_INTERVAL_DAY:
            case Types.SQL_INTERVAL_DAY_TO_HOUR:
            case Types.SQL_INTERVAL_DAY_TO_MINUTE:
            case Types.SQL_INTERVAL_DAY_TO_SECOND:
            case Types.SQL_INTERVAL_HOUR:
            case Types.SQL_INTERVAL_HOUR_TO_MINUTE:
            case Types.SQL_INTERVAL_HOUR_TO_SECOND:
            case Types.SQL_INTERVAL_MINUTE:
            case Types.SQL_INTERVAL_MINUTE_TO_SECOND:
            case Types.SQL_INTERVAL_SECOND:
                throw new IllegalArgumentException(
                    "Driver doesn't support type 'INTERVAL' yet");
                /*  Haven't figured out how the client expects the atttypmod to
                 *  be calculated for intervals.
                return new PgType(hType, TYPE_TINTERVAL, 16, ?hType.precision?);
                */

            default:
                throw new IllegalArgumentException(
                    "Unsupported type: " + hType.getNameString());
        }
    }

    /**
     * This method copied from JDBCPreparedStatement.java.
     *
     * The internal parameter value setter always converts the parameter to
     * the Java type required for data transmission.
     *
     * @param i parameter index
     * @param o object
     * @throws SQLException if either argument is not acceptable.
     */
    public Object getParameter(String inString, Session session)
    throws SQLException {
        if (inString == null) {
            return null;
        }
        Object o = inString;

        switch (hType.typeCode) {

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            case Types.OTHER :
            case Types.SQL_BLOB :
            case Types.SQL_CLOB :
                throw new IllegalArgumentException(
                        "Type not supported yet: " + hType.getNameString());
                /*
            case Types.OTHER :
                try {
                    if (o instanceof Serializable) {
                        o = new JavaObjectData((Serializable) o);

                        break;
                    }
                } catch (HsqlException e) {
                    PgType.throwError(e);
                }
                PgType.throwError(Error.error(ErrorCode.X_42565));

                break;
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                if (o instanceof byte[]) {
                    o = new BinaryData((byte[]) o, false);

                    break;
                }
                PgType.throwError(Error.error(ErrorCode.X_42565));

                break;
            case Types.SQL_BLOB :
                //setBlobParameter(i, o);

                //break;
            case Types.SQL_CLOB :
                //setClobParameter(i, o);

                //break;
            */
            case Types.SQL_DATE :
            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIME :
            case Types.SQL_TIMESTAMP : {
                try {
                    if (o instanceof String) {
                        o = hType.convertToType(session, o, Type.SQL_VARCHAR);

                        break;
                    }
                    o = hType.convertJavaToSQL(session, o);

                    break;
                } catch (HsqlException e) {
                    PgType.throwError(e);
                }
            }

            // fall through
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                try {
                    if (o instanceof String) {
                        o = hType.convertToType(session, o, Type.SQL_VARCHAR);

                        break;
                    }
                    o = hType.convertToDefaultType(session, o);

                    break;
                } catch (HsqlException e) {
                    PgType.throwError(e);
                }

            /*
            default :
                throw new IllegalArgumentException(
                    "Parameter value is of unexpected type: "
                    + hType.getNameString());
            */
            // fall through
                try {
                    o = hType.convertToDefaultType(session, o);
                    // TODO:  Is this desirable?
                    // Probably better to throw.

                    break;
                } catch (HsqlException e) {
                    PgType.throwError(e);
                }
        }
        return o;
    }

    /*
     * The followign settings are a Java port of pgtypes.h
     */
    public static final int TYPE_BOOL         =  16;
    public static final int TYPE_BYTEA        =  17;
    public static final int TYPE_CHAR         =  18;
    public static final int TYPE_NAME         =  19;
    public static final int TYPE_INT8         =  20;
    public static final int TYPE_INT2         =  21;
    public static final int TYPE_INT2VECTOR   =  22;
    public static final int TYPE_INT4         =  23;
    public static final int TYPE_REGPROC      =  24;
    public static final int TYPE_TEXT         =  25;
    public static final int TYPE_OID          =  26;
    public static final int TYPE_TID          =  27;
    public static final int TYPE_XID          =  28;
    public static final int TYPE_CID          =  29;
    public static final int TYPE_OIDVECTOR    =  30;
    public static final int TYPE_SET          =  32;
    public static final int TYPE_XML          = 142;
    public static final int TYPE_XMLARRAY     = 143;
    public static final int TYPE_CHAR2        = 409;
    public static final int TYPE_CHAR4        = 410;
    public static final int TYPE_CHAR8        = 411;
    public static final int TYPE_POINT        = 600;
    public static final int TYPE_LSEG         = 601;
    public static final int TYPE_PATH         = 602;
    public static final int TYPE_BOX          = 603;
    public static final int TYPE_POLYGON      = 604;
    public static final int TYPE_FILENAME     = 605;
    public static final int TYPE_CIDR         = 650;
    public static final int TYPE_FLOAT4       = 700;
    public static final int TYPE_FLOAT8       = 701;
    public static final int TYPE_ABSTIME      = 702;
    public static final int TYPE_RELTIME      = 703;
    public static final int TYPE_TINTERVAL    = 704;
    public static final int TYPE_UNKNOWN      = 705;
    public static final int TYPE_MONEY        = 790;
    public static final int TYPE_OIDINT2      = 810;
    public static final int TYPE_MACADDR      = 829;
    public static final int TYPE_INET         = 869;
    public static final int TYPE_OIDINT4      = 910;
    public static final int TYPE_OIDNAME      = 911;
    public static final int TYPE_TEXTARRAY    = 1009;
    public static final int TYPE_BPCHARARRAY  = 1014;
    public static final int TYPE_VARCHARARRAY = 1015;
    public static final int TYPE_BPCHAR       = 1042;
    public static final int TYPE_VARCHAR      = 1043;
    public static final int TYPE_DATE         = 1082;
    public static final int TYPE_TIME         = 1083;
    public static final int TYPE_TIMESTAMP_NO_TMZONE = 1114; /* since 7.2 */
    public static final int TYPE_DATETIME     = 1184;
    public static final int TYPE_TIME_WITH_TMZONE   = 1266;   /* since 7.1 */
    public static final int TYPE_TIMESTAMP    = 1296; /* deprecated since 7.0 */
    public static final int TYPE_NUMERIC      = 1700;
    public static final int TYPE_RECORD       = 2249;
    public static final int TYPE_VOID         = 2278;
    public static final int TYPE_UUID         = 2950;

    // Apparenly new additions, from Postgresql server file pg_type.h:
    public static final int TYPE_BIT          = 1560;
    // Also defined is _bit.  No idea what that is about
    public static final int TYPE_VARBIT       = 1562;
    // Also defined is _varbit.  No idea what that is about

    /* Following stuff is to support code copied from
     * JDBCPreparedStatement.java. */
    static final void throwError(HsqlException e) throws SQLException {

//#ifdef JAVA6
        throw Util.sqlException(e.getMessage(), e.getSQLState(),
            e.getErrorCode(), e);

//#else
/*
        throw new SQLException(e.getMessage(), e.getSQLState(),
                               e.getErrorCode());
*/

//#endif JAVA6
    }

    static protected final PgType int2singleton =
        new PgType(Type.SQL_SMALLINT, TYPE_INT2, 2);
    static protected final PgType int4singleton =
        new PgType(Type.SQL_INTEGER, TYPE_INT4, 4);
    static protected final PgType int8singleton =
        new PgType(Type.SQL_BIGINT, TYPE_INT8, 8);
    static protected final PgType doubleSingleton =
        new PgType(Type.SQL_DOUBLE, TYPE_FLOAT8, 8);
    static protected final PgType boolSingleton =
        new PgType(Type.SQL_BOOLEAN, TYPE_BOOL, 1);
    static protected final PgType textSingleton =
        new PgType(Type.SQL_VARCHAR, TYPE_TEXT);
    static protected final PgType dateSingleton =
        new PgType(Type.SQL_DATE, TYPE_DATE, 4);
    static protected final PgType unknownSingleton =
        new PgType(Type.SQL_CHAR, TYPE_UNKNOWN, -2);
    static protected final PgType bitSingleton =
        new PgType(Type.SQL_BIT, TYPE_BIT);
    static protected final PgType bitVaryingSingleton =
        new PgType(Type.SQL_BIT_VARYING, TYPE_VARBIT);
}
