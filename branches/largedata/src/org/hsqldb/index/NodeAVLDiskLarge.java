package org.hsqldb.index;

import java.io.IOException;

import org.hsqldb.Row;
import org.hsqldb.RowAVLDisk;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.LongLookup;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// fredt@users 20020920 - path 1.7.1 - refactoring to cut mamory footprint
// fredt@users 20021205 - path 1.7.2 - enhancements

/**
 *  Cached table Node implementation.<p>
 *  Only integral references to left, right and parent nodes in the AVL tree
 *  are held and used as pointers data.<p>
 *
 *  iId is a reference to the Index object that contains this node.<br>
 *  This fields can be eliminated in the future, by changing the
 *  method signatures to take a Index parameter from Index.java (fredt@users)
 *
 *  New class derived from Hypersonic SQL code and enhanced in HSQLDB. <p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge dot net)
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 2.3.0
 * @since Hypersonic SQL
 */
public class NodeAVLDiskLarge extends NodeAVL {

    final RowAVLDisk row;

    //
    private long            iLeft   = NO_POS;
    private long            iRight  = NO_POS;
    private long            iParent = NO_POS;
    private int             iId;    // id of Index object for this Node
    public static final int SIZE_IN_BYTE = 4 * 4;

    public NodeAVLDiskLarge(RowAVLDisk r, RowInputInterface in,
                       int id) throws IOException {

        int ext;

        row      = r;
        iId      = id;
        ext      = in.readInt();
        iBalance = (byte) ext;
        iLeft    = in.readInt() & 0xffffffffL;
        iRight   = in.readInt() & 0xffffffffL;
        iParent  = in.readInt() & 0xffffffffL;

        if (ext > 0xff) {
            iParent |= (((long) ext << 8) & 0xff00000000L);
            iLeft   |= (((long) ext << 16) & 0xff00000000L);
            iRight  |= (((long) ext << 24) & 0xff00000000L);
        }

        if (iLeft == 0) {
            iLeft = NO_POS;
        }

        if (iRight == 0) {
            iRight = NO_POS;
        }

        if (iParent == 0) {
            iParent = NO_POS;
        }
    }

    public NodeAVLDiskLarge(RowAVLDisk r, int id) {
        row = r;
        iId = id;
    }

    public void delete() {

        iLeft    = NO_POS;
        iRight   = NO_POS;
        iParent  = NO_POS;
        nLeft    = null;
        nRight   = null;
        nParent  = null;
        iBalance = 0;

        row.setNodesChanged();
    }

    public boolean isInMemory() {
        return row.isInMemory();
    }

    public boolean isMemory() {
        return false;
    }

    public long getPos() {
        return row.getPos();
    }

    public Row getRow(PersistentStore store) {

        if (!row.isInMemory()) {
            return (RowAVLDisk) store.get(this.row, false);
        } else {
            row.updateAccessCount(store.getAccessCount());
        }

        return row;
    }

    public Object[] getData(PersistentStore store) {
        return row.getData();
    }

    private NodeAVLDiskLarge findNode(PersistentStore store, long pos) {

        NodeAVLDiskLarge ret = null;
        RowAVLDisk  r   = (RowAVLDisk) store.get(pos, false);

        if (r != null) {
            ret = (NodeAVLDiskLarge) r.getNode(iId);
        }

        return ret;
    }

    boolean isLeft(NodeAVL n) {

        if (n == null) {
            return iLeft == NO_POS;
        }

        return iLeft == n.getPos();
    }

    boolean isRight(NodeAVL n) {

        if (n == null) {
            return iRight == NO_POS;
        }

        return iRight == n.getPos();
    }

    NodeAVL getLeft(PersistentStore store) {

        NodeAVLDiskLarge node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDiskLarge) row.getNode(iId);
        }

        if (node.iLeft == NO_POS) {
            return null;
        }

        if (node.nLeft == null || !node.nLeft.isInMemory()) {
            node.nLeft         = findNode(store, node.iLeft);
            node.nLeft.nParent = node;
        }

        return node.nLeft;
    }

    NodeAVL getRight(PersistentStore store) {

        NodeAVLDiskLarge node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDiskLarge) row.getNode(iId);
        }

        if (node.iRight == NO_POS) {
            return null;
        }

        if (node.nRight == null || !node.nRight.isInMemory()) {
            node.nRight         = findNode(store, node.iRight);
            node.nRight.nParent = node;
        }

        return node.nRight;
    }

    NodeAVL getParent(PersistentStore store) {

        NodeAVLDiskLarge node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDiskLarge) row.getNode(iId);
        }

        if (node.iParent == NO_POS) {
            return null;
        }

        if (node.nParent == null || !node.nParent.isInMemory()) {
            node.nParent = findNode(store, iParent);
        }

        return node.nParent;
    }

    public int getBalance(PersistentStore store) {

        NodeAVLDiskLarge node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDiskLarge) row.getNode(iId);
        }

        return node.iBalance;
    }

    boolean isRoot(PersistentStore store) {

        NodeAVLDiskLarge node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDiskLarge) row.getNode(iId);
        }

        return node.iParent == NO_POS;
    }

    boolean isFromLeft(PersistentStore store) {

        NodeAVLDiskLarge node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDiskLarge) row.getNode(iId);
        }

        if (node.iParent == NO_POS) {
            return true;
        }

        if (node.nParent == null || !node.nParent.isInMemory()) {
            node.nParent = findNode(store, iParent);
        }

        return row.getPos() == ((NodeAVLDiskLarge) node.nParent).iLeft;
    }

    public NodeAVL child(PersistentStore store, boolean isleft) {
        return isleft ? getLeft(store)
                      : getRight(store);
    }

    NodeAVL setParent(PersistentStore store, NodeAVL n) {

        NodeAVLDiskLarge node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDiskLarge) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            row.keepInMemory(false);

            throw Error.runtimeError(ErrorCode.U_S0500, "NodeAVLDisk");
        }

        row.setNodesChanged();

        node.iParent = n == null ? NO_POS
                                 : n.getPos();

        if (n != null && !n.isInMemory()) {
            n = findNode(store, n.getPos());
        }

        node.nParent = (NodeAVLDiskLarge) n;

        row.keepInMemory(false);

        return node;
    }

    public NodeAVL setBalance(PersistentStore store, int b) {

        NodeAVLDiskLarge node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDiskLarge) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            throw Error.runtimeError(ErrorCode.U_S0500, "NodeAVLDisk");
        }

        row.setNodesChanged();

        node.iBalance = b;

        row.keepInMemory(false);

        return node;
    }

    NodeAVL setLeft(PersistentStore store, NodeAVL n) {

        NodeAVLDiskLarge node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDiskLarge) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            throw Error.runtimeError(ErrorCode.U_S0500, "NodeAVLDisk");
        }

        row.setNodesChanged();

        node.iLeft = n == null ? NO_POS
                               : n.getPos();

        if (n != null && !n.isInMemory()) {
            n = findNode(store, n.getPos());
        }

        node.nLeft = (NodeAVLDiskLarge) n;

        row.keepInMemory(false);

        return node;
    }

    NodeAVL setRight(PersistentStore store, NodeAVL n) {

        NodeAVLDiskLarge node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDiskLarge) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            throw Error.runtimeError(ErrorCode.U_S0500, "NodeAVLDisk");
        }

        row.setNodesChanged();

        node.iRight = n == null ? NO_POS
                                : n.getPos();

        if (n != null && !n.isInMemory()) {
            n = findNode(store, n.getPos());
        }

        node.nRight = (NodeAVLDiskLarge) n;

        row.keepInMemory(false);

        return node;
    }

    public NodeAVL set(PersistentStore store, boolean isLeft, NodeAVL n) {

        NodeAVL x;

        if (isLeft) {
            x = setLeft(store, n);
        } else {
            x = setRight(store, n);
        }

        if (n != null) {
            n.setParent(store, this);
        }

        return x;
    }

    public void replace(PersistentStore store, Index index, NodeAVL n) {

        NodeAVLDiskLarge node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDiskLarge) row.getNode(iId);
        }

        if (node.iParent == NO_POS) {
            if (n != null) {
                n = n.setParent(store, null);
            }

            store.setAccessor(index, n);
        } else {
            boolean isFromLeft = node.isFromLeft(store);

            node.getParent(store).set(store, isFromLeft, n);
        }

        row.keepInMemory(false);
    }

    boolean equals(NodeAVL n) {

        if (n instanceof NodeAVLDiskLarge) {
            return this == n || row.getPos() == ((NodeAVLDiskLarge) n).getPos();
        }

        return false;
    }

    public int getRealSize(RowOutputInterface out) {
        return NodeAVLDiskLarge.SIZE_IN_BYTE;
    }

    public void setInMemory(boolean in) {

        if (!in) {
            if (nLeft != null) {
                nLeft.nParent = null;
            }

            if (nRight != null) {
                nRight.nParent = null;
            }

            if (nParent != null) {
                if (row.getPos() == ((NodeAVLDiskLarge) nParent).iLeft) {
                    nParent.nLeft = null;
                } else {
                    nParent.nRight = null;
                }
            }

            nLeft = nRight = nParent = null;
        }
    }

    public void write(RowOutputInterface out) {
        write(out, null);
    }

    public void write(RowOutputInterface out, LongLookup lookup) {

        long leftTemp   = getTranslatePointer(iLeft, lookup);
        long rightTemp  = getTranslatePointer(iRight, lookup);
        long parentTemp = getTranslatePointer(iParent, lookup);
        int  ext        = 0;

        ext |= (int) ((parentTemp & 0xff00000000L) >> 8);
        ext |= (int) ((leftTemp & 0xff00000000L) >> 16);
        ext |= (int) ((rightTemp & 0xff00000000L) >> 24);

        if (ext == 0) {
            ext = iBalance;
        } else {
            ext |= (iBalance & 0xff);
        }

        out.writeInt(ext);
        out.writeInt((int) leftTemp);
        out.writeInt((int) rightTemp);
        out.writeInt((int) parentTemp);
    }

    private static long getTranslatePointer(long pointer, LongLookup lookup) {

        long newPointer = 0;

        if (pointer != NodeAVL.NO_POS) {
            if (lookup == null) {
                newPointer = pointer;
            } else {
                newPointer = lookup.lookup(pointer);
            }
        }

        return newPointer;
    }

    public void restore() {}

    public void destroy() {}

    public void updateAccessCount(int count) {}

    public int getAccessCount() {
        return 0;
    }

    public void setStorageSize(int size) {}

    public int getStorageSize() {
        return 0;
    }

    public void setPos(long pos) {}

    public boolean hasChanged() {
        return false;
    }

    public boolean isKeepInMemory() {
        return false;
    }

    public boolean keepInMemory(boolean keep) {
        return false;
    }
}
