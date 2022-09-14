package org.hsqldb.testbase.jtm;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.xa.Xid;

// @todo - full corba / idl spec

/**
 * Provides an implementation of the X/Open transaction identifier.
 * <p>
 * Internally, the XOpenXID is made up of two contiguous parts. The first (of
 * size <b>gtrid</b>_length) is the global transaction identifier and the second
 * (of size <b>bqual</b>_length) is the branch qualifier.
 * <p>
 * If the formatID is -1, indicating the NULLXID, the data is ignored.
 */
public class XOpenXID implements Xid {

    private static final int XIDDATASIZE = MAXGTRIDSIZE + MAXBQUALSIZE; // Size in bytes

    private static final Logger LOG = Logger.getLogger(XOpenXID.class.getName());
    private static final int NULL_XID = -1;

    //-----------------------------------------------------------------------//
    // Data Area                                                             //
    //-----------------------------------------------------------------------//
    /**
     * The format identifier for the XID. A value of -1 indicates that the
     * NULLXID.
     */
    private int formatID;   // Format identifier
    private int gtridLength;  // Value from 1 through MAXGTRIDSIZE
    private int bqualLength;  // Value from 1 through MAXBQUALSIZE
    private final byte data[];       // The XOpenXID data (size XIDDATASIZE)

    //-----------------------------------------------------------------------//
    // XOpenXID::Constructor                                                      //
    //-----------------------------------------------------------------------//
    /**
     * Constructs a new null XID.
     * <p>
     * After construction, the data within the XID should be initialized.
     */
    public XOpenXID() {
        data = new byte[XIDDATASIZE];
        formatID = NULL_XID;
    }

    //-----------------------------------------------------------------------//
    // XOpenXID::Methods                                                          //
    //-----------------------------------------------------------------------//
    /**
     * Initialize an XOpenXID using another XOpenXID as the source of data.
     *
     * @param from the XOpenXID to initialize this XOpenXID from
     *
     */
    @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
    public void copy(XOpenXID from) {

        formatID = NULL_XID;
        if (from == null) {
            return;
        }

        if (from.formatID == NULL_XID) {
            return;
        }

        gtridLength = from.gtridLength;
        bqualLength = from.bqualLength;

        if (data != null && from.data != null) {
            System.arraycopy(from.data, 0, data, 0, XIDDATASIZE);
        }

        formatID = from.formatID;         // Last, in case of failure
    }


    /*
     * Are the XIDs equal?
     */
    /**
     * Determine whether or not two objects of this type are equal.
     *
     * @param o the object to be compared with this XOpenXID.
     *
     * @return Returns true of the supplied object represents the same global
     *         transaction as this, otherwise returns false.
     */
    @Override
    @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        final XOpenXID other = o == null || o.getClass() != XOpenXID.class
                ? null
                : (XOpenXID) o;
        if (other == null) {
            return false;
        }

        if (formatID == NULL_XID && other.formatID == NULL_XID) {
            return true;
        }

        if (formatID != other.formatID) {
            return false;
        }

        final int len = gtridLength + bqualLength;
        for (int i = len - 1; i >= 0; i--) {
            if (data[i] != other.data[i]) {
                return false;
            }
        }

        return true;
    }

    /*
     * Compute the hash code.
     */
    /**
     * Compute the hash code.
     *
     * @return the computed hashcode
     */
    @Override
    public int hashCode() {
        if (formatID == (NULL_XID)) {
            return (NULL_XID);
        }

        return formatID + gtridLength - bqualLength;

    }

    /**
     * Return a string representing this XOpenXID.
     *
     * @return the string representation of this XOpenXID
     */
    @Override
    public String toString() {
        return LOG.isLoggable(Level.INFO)
                ? String.format("%s@%X",
                        XOpenXID.class.getSimpleName(),
                        System.identityHashCode(this))
                : LOG.isLoggable(Level.FINE)
                ? toDebugString()
                : XOpenXID.class.getSimpleName();
    }

    public String toDebugString() {
        final String hex = "0123456789ABCDEF";
        final int len = gtridLength + bqualLength;
        final StringBuffer sb = new StringBuffer(len + len);
        for (int i = 0; i < len; i++) {
            int value = data[i] & 0xff;
            sb.append(hex.charAt(value / 16));
            sb.append(hex.charAt(value & 15));
            if ((i + 1) % 4 == 0 && (i + 1) < len) {
                sb.append(" ");
            }
        }

        return "XID{"
                + "formatID(" + formatID + "), "
                + "gtrid_length(" + gtridLength + "), "
                + "bqual_length(" + bqualLength + "), "
                + "data(" + sb + ")"
                + "}";
    }

    /*
     * Return branch qualifier
     */
    /**
     * Returns the branch qualifier for this XOpenXID.
     *
     * @return the branch qualifier
     */
    @Override
    public byte[] getBranchQualifier() {
        final byte[] bqual = new byte[bqualLength];
        System.arraycopy(data, gtridLength, bqual, 0, bqualLength);
        return bqual;
    }

    /**
     * Set the branch qualifier for this XOpenXID.
     *
     * @param bytes contains the branch qualifier to be set. If the size of the
     *              array exceeds MAXBQUALSIZE, only the first MAXBQUALSIZE elements will be
     *              used.
     */
    public void setBranchQualifier(byte[] bytes) {
        bqualLength = bytes.length > MAXBQUALSIZE
                ? MAXBQUALSIZE
                : bytes.length;
        System.arraycopy(bytes, 0, data, gtridLength, bqualLength);
    }

    /**
     * Obtain the format identifier part of the XOpenXID.
     *
     * @return Format identifier. -1 indicates a null XOpenXID
     */
    @Override
    public int getFormatId() {
        return formatID;
    }

    /**
     * Set the format identifier part of the XOpenXID.
     *
     * @param formatID identifier. -1 indicates a null Xid.
     */
    public void setFormatId(int formatID) {
        this.formatID = formatID;
    }

    /*
     * Determine if an array of bytes equals the branch qualifier
     */
    /**
     * Compares the input parameter with the branch qualifier for equality.
     *
     * @param data
     * @return true if equal
     */
    public boolean isEqualBranchQualifier(byte[] data) {

        int len = data.length > MAXBQUALSIZE
                ? MAXBQUALSIZE
                : data.length;
        int i;

        if (len != bqualLength) {
            return false;
        }

        for (i = 0; i < len; i++) {
            if (data[i] != this.data[gtridLength + i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Return whether the Gtrid of this is equal to the Gtrid of xid
     *
     * @param xid
     * @return
     */
    @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
    public boolean isEqualGtrid(XOpenXID xid) {
        if (this.gtridLength != xid.gtridLength) {
            return false;
        }

        for (int i = 0; i < gtridLength; i++) {
            if (this.data[i] != xid.data[i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns a copy of the global transaction identifier for this XOpenXID.
     *
     * @return the global transaction identifier
     */
    @Override
    public byte[] getGlobalTransactionId() {

        final byte[] gtrid = new byte[gtridLength];
        System.arraycopy(data, 0, gtrid, 0, gtridLength);

        return gtrid;
    }

}
