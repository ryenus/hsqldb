/*
 * $Id$
 */

/**
 * Java port of pgtypes.h
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
    /* TODO:  Consider designating the binary types in this class */
    private int oid;
    private int typeSize = -1;
    private int constraintSize = -1;
    private Type hType;

    public int getOid() {
        return oid;
    }
    public int getTypeSize() {
        return typeSize;
    }
    public int getConstraintSize() {
        return constraintSize;
    }

    public PgType(Type hType) {
        this.hType = hType;
        if (hType instanceof NumberType) {
            NumberType numType = (NumberType) hType;
            typeSize = numType.getPrecision() / 8;
            // TODO:  Figure out how to get decimal precision, etc.
            //
            if (numType.isIntegralType()) {
                switch (typeSize) {
                    case 2:
                        oid = TYPE_INT2;
                        break;
                    case 4:
                        oid = TYPE_INT4;
                        break;
                    case 8:
                        oid = TYPE_INT8;
                        break;
                    default:
                        throw new RuntimeException(
                                "Unsupported type: " + hType);
                }
            } else {
                switch (typeSize) {
                    case 4:
                        oid = TYPE_FLOAT4;
                        break;
                    case 8:
                        oid = TYPE_FLOAT8;
                        break;
                    default:
                        throw new RuntimeException(
                                "Unsupported type: " + hType);
                }
            }
        } else if (hType instanceof BooleanType) {
            oid = TYPE_BOOL;
            typeSize = 1;
        } else if (hType instanceof CharacterType) {
            CharacterType charType = (CharacterType) hType;
            oid = charType.requiresPrecision() ? TYPE_VARCHAR : TYPE_BPCHAR;
        } else if (hType instanceof DateTimeType) {
            oid = TYPE_DATE;
            typeSize = 4;
            // The times are size 8, but I don't know how to differentiate.
            //TYPE_TIME           1083
            //TYPE_DATETIME       1184
            //TYPE_TIMESTAMP_NO_TMZONE 1114  /* since 7.2 */
            //TYPE_TIME_WITH_TMZONE   1266    /* since 7.1 */
            //TYPE_TINTERVAL      704
        /*
        } else if (hType instanceof BitType) {
        } else if (hType instanceof BinaryType) {
        } else if (hType instanceof BlobType) {
        } else if (hType instanceof ClobType) {
        */
        } else {
            throw new RuntimeException("Unsupported type: " + hType);
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
                throw new RuntimeException("Type not supported yet: " + hType);

                //break;
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

            // fall through
            default :
                try {
                    o = hType.convertToDefaultType(session, o);

                    break;
                } catch (HsqlException e) {
                    PgType.throwError(e);
                }
        }
        return o;
    }

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

    static final void throwError(HsqlException e) throws SQLException {

//#ifdef JAVA6
        throw Util.sqlException(e.getMessage(), e.getSQLState(), e.getErrorCode(),
                           e);

//#else
/*
        throw new SQLException(e.getMessage(), e.getSQLState(),
                               e.getErrorCode());
*/

//#endif JAVA6
    }
}
