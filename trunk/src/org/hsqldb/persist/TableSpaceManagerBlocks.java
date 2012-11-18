/* Copyright (c) 2001-2011, The HSQL Development Group
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

import org.hsqldb.lib.DoubleIntIndex;
import org.hsqldb.lib.ArrayUtil;

/**
 * Manages allocation of space for rows.<p>
 * Maintains a list of free file blocks with fixed capacity.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.3.0
 */
public class TableSpaceManagerBlocks implements TableSpaceManager {

    DataSpaceManagerBlocks spaceManager;
    private int            scale;
    int                    mainBlockSize;
    int                    spaceID;

    //
    private DoubleIntIndex lookup;
    private final int      capacity;
    private int            midSize;
    private long           releaseCount;
    private long           requestCount;
    private long           requestSize;

    // reporting vars
    long    freeBlockSize;
    long    lostFreeBlockSize;
    boolean isModified;

    //
    long freshBlockPos     = 0;
    long freshBlockFreePos = 0;
    long freshBlockLimit   = 0;

    /**
     *
     */
    public TableSpaceManagerBlocks(DataSpaceManagerBlocks spaceManager,
                                   int tableId, int fileBlockSize,
                                   int capacity, int fileScale,
                                   long lostSize) {

        this.spaceManager  = spaceManager;
        this.scale         = fileScale;
        this.spaceID       = tableId;
        this.mainBlockSize = fileBlockSize;
        lookup             = new DoubleIntIndex(capacity, true);

        lookup.setValuesSearchTarget();

        this.capacity          = capacity;
        this.lostFreeBlockSize = lostSize;
        this.midSize           = 128;    // arbitrary initial value
    }

    public boolean hasFileRoom(int blockSize) {
        return freshBlockLimit - freshBlockFreePos > blockSize;
    }

    public void addFileBlock(long blockPos, long blockFreePos,
                             long blockLimit) {

        int released = (int) (freshBlockLimit - freshBlockFreePos);

        if (released > 0) {
            release(freshBlockFreePos / scale, released);
        }

        initialiseFileBlock(blockPos, blockFreePos, blockLimit);
    }

    public void initialiseFileBlock(long blockPos, long blockFreePos,
                                    long blockLimit) {

        this.freshBlockPos     = blockPos;
        this.freshBlockFreePos = blockFreePos;
        this.freshBlockLimit   = blockLimit;
    }

    boolean getNewMainBlock(int rowSize) {

        long released = freshBlockLimit - freshBlockFreePos;

        if (released > 0) {
            release(freshBlockFreePos / scale, (int) released);
        }

        int blockSize  = mainBlockSize;
        int blockCount = 1;

        while (blockSize < rowSize) {
            blockSize += blockSize;

            blockCount++;
        }

        long position = spaceManager.getFileSpace(spaceID, blockCount);

        if (position < 0) {
            return false;
        }

        freshBlockPos     = position;
        freshBlockFreePos = position;
        freshBlockLimit   = position + blockSize;

        return true;
    }

    long getNewBlock(int rowSize, boolean asBlocks) {

        long position = freshBlockFreePos;

        if (asBlocks) {
            position = ArrayUtil.getBinaryMultipleCeiling(position,
                    DataSpaceManager.fixedBlockSizeUnit);

            long released = position - freshBlockFreePos;

            if (released > 0) {
                release(freshBlockFreePos / scale, (int) released);
            }

            freshBlockFreePos = position;
            rowSize = (int) ArrayUtil.getBinaryMultipleCeiling(rowSize,
                    DataSpaceManager.fixedBlockSizeUnit);
        }

        if (rowSize > freshBlockLimit - freshBlockFreePos) {
            boolean result = getNewMainBlock(rowSize);

            if (!result) {
                return -1;
            }

            position = freshBlockFreePos;
        }

        freshBlockFreePos += rowSize;

        return position / scale;
    }

    public int getSpaceID() {
        return spaceID;
    }

    /**
     */
    synchronized public void release(long pos, int rowSize) {

        isModified = true;

        // test code
        if (rowSize > 4000) {

//            System.out.println(pos + "  " + rowSize);
        }

        if (capacity == 0) {
            lostFreeBlockSize += rowSize;

            return;
        }

        releaseCount++;

        //
        if (lookup.size() == capacity) {
            resetList();
        }

        if (pos < Integer.MAX_VALUE) {
            lookup.add((int) pos, rowSize);

            freeBlockSize += rowSize;
        }
    }

    /**
     * Returns the position of a free block or 0.
     */
    synchronized public long getFilePosition(int rowSize, boolean asBlocks) {

        if (capacity == 0) {
            return getNewBlock(rowSize, asBlocks);
        }

        // todo asBlocks for reuse
        if (asBlocks) {
            return getNewBlock(rowSize, asBlocks);
        }

        int index = lookup.findFirstGreaterEqualKeyIndex(rowSize);

        if (index == -1) {
            return getNewBlock(rowSize, asBlocks);
        }

        // statistics for successful requests only - to be used later for midSize
        requestCount++;

        requestSize += rowSize;

        int length     = lookup.getValue(index);
        int difference = length - rowSize;
        int key        = lookup.getKey(index);

        lookup.remove(index);

        if (difference >= midSize) {
            int pos = key + (rowSize / scale);

            lookup.add(pos, difference);

            freeBlockSize -= rowSize;
        } else {
            lostFreeBlockSize += difference;
            freeBlockSize     -= length;
        }

        return key;
    }

    public int freeBlockCount() {
        return lookup.size();
    }

    public long freeBlockSize() {
        return freeBlockSize;
    }

    public long getLostBlocksSize() {
        return lostFreeBlockSize;
    }

    public boolean isModified() {
        return isModified;
    }

    private void resetList() {

        if (requestCount != 0) {
            midSize = (int) (requestSize / requestCount);
        }

        int first = lookup.findFirstGreaterEqualSlotIndex(midSize);

        if (first < lookup.size() / 4) {
            first = lookup.size() / 4;
        }

        removeBlocks(first);
    }

    private void removeBlocks(int blocks) {

        for (int i = 0; i < blocks; i++) {
            lostFreeBlockSize += lookup.getValue(i);
            freeBlockSize     -= lookup.getValue(i);
        }

        lookup.removeRange(0, blocks);
    }
}
