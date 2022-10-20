package org.hsqldb.testbase.jtm;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import javax.transaction.xa.Xid;

/**
 * Implementation of Xid for tests.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
public class MockXid implements Xid {

    private static final AtomicInteger TXN_SEQUENCE = new AtomicInteger();
    //
    private static final int UXID_FORMAT_ID = 0xFEED;

    private static int nextTxnSequenceNumber() {
        return TXN_SEQUENCE.getAndIncrement();
    }

    private static byte[] getLocalHostIpBytes() {
        try {
            return InetAddress.getLocalHost().getAddress();
        } catch (UnknownHostException ex) {
            return InetAddress.getLoopbackAddress().getAddress();
        }
    }
    
        public static Xid getUniqueXid() {
          return getUniqueXid(Thread.currentThread().getId());
        }


    /**
     * Retrieves a randomly generated JDBCXID.
     *
     * The newly generated object is based on the local IP address, the given
     * <code>threadId</code> and a randomly generated number using the current
     * time in milliseconds as the random seed.
     *
     * Note that java.util.Random is used, not java.security.SecureRandom.
     *
     * @param threadId can be a real thread id or just some convenient tracking
     * value.
     *
     * @return a randomly generated JDBCXID
     */
    public static Xid getUniqueXid(final long threadId) {

        final Random random = new Random(System.currentTimeMillis());

        // 4 bytes
        int tsn = nextTxnSequenceNumber();
        // 8 bytes
        long tid = threadId;
        // 4 bytes
        byte[] ipBytes = getLocalHostIpBytes();
        // 4 bytes
        long randomValue = random.nextLong();

        // 64 bytes
        byte[] globalTransactionId = new byte[MAXGTRIDSIZE];
        // 64 bytes
        byte[] branchQualifier = new byte[MAXBQUALSIZE];
       

        // bytes 0 -> 3 host ip bytes (big endian)
        System.arraycopy(ipBytes, 0, globalTransactionId, 0, 4);
        System.arraycopy(ipBytes, 0, branchQualifier, 0, 4);

        // Bytes 4 -> 7 - transaction sequence number.
        // Bytes 8 ->11 - thread id.
        // Bytes 12->15 - random.
        for (int i = 0; i <= 3; i++) {
            globalTransactionId[i + 4] = (byte) (tsn
                    % 0x100);
            branchQualifier[i + 4] = (byte) (tsn % 0x100);
            tsn >>= 8;
            globalTransactionId[i + 8] = (byte) (tid % 0x100);
            branchQualifier[i + 8] = (byte) (tid % 0x100);
            tid >>= 8;
            globalTransactionId[i + 12] = (byte) (randomValue % 0x100);
            branchQualifier[i + 12] = (byte) (randomValue % 0x100);
            randomValue >>= 8;
        }

        return new MockXid(UXID_FORMAT_ID, globalTransactionId,
                branchQualifier);
    }

    int formatID;
    byte[] txID;
    byte[] txBranch;

    //
    int hash;
    boolean hashComputed;

    public MockXid(int formatID, byte[] txID, byte[] txBranch) {

        this.formatID = formatID;
        this.txID = txID;
        this.txBranch = txBranch;
    }

    @Override
    public int getFormatId() {
        return formatID;
    }

    @Override
    public byte[] getGlobalTransactionId() {
        return txID;
    }

    @Override
    public byte[] getBranchQualifier() {
        return txBranch;
    }

    @Override
    public int hashCode() {

        if (!hashComputed) {
            hash = 7;
            hash = 83 * hash + this.formatID;
            hash = 83 * hash + Arrays.hashCode(this.txID);
            hash = 83 * hash + Arrays.hashCode(this.txBranch);
            hashComputed = true;
        }

        return hash;
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof Xid) {
            Xid o = (Xid) other;

            return formatID == o.getFormatId()
                    && Arrays.equals(txID, o.getGlobalTransactionId())
                    && Arrays.equals(txBranch, o.getBranchQualifier());
        }

        return false;
    }

    // inherit doc
    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder(512);

        //
        sb.append("formatId=").append(getFormatId());

        //
        sb.append(" globalTransactionId(").append(txID.length).append(")={0x");

        for (int i = 0; i < txID.length; i++) {
            final int hexVal = txID[i] & 0xFF;

            if (hexVal < 0x10) {
                sb.append("0").append(Integer.toHexString(txID[i] & 0xFF));
            }

            sb.append(Integer.toHexString(txID[i] & 0xFF));
        }

        //
        sb.append("} branchQualifier(").append(txBranch.length).append(
                "))={0x");

        for (int i = 0; i < txBranch.length; i++) {
            final int hexVal = txBranch[i] & 0xFF;

            if (hexVal < 0x10) {
                sb.append("0");
            }

            sb.append(Integer.toHexString(txBranch[i] & 0xFF));
        }

        sb.append("}");

        //
        return sb.toString();
    }

}
