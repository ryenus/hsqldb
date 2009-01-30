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

public class PgType {
    /* TODO:  Consider designating the binary types in this class */
    private int oid;
    private int typeSize = -1;
    private int constraintSize = -1;

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
}
