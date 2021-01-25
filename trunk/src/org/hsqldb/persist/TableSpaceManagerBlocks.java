/* Copyright (c) 2001-2021, The HSQL Development Group
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

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.DoubleIntIndex;
import org.hsqldb.lib.DoubleLongIndex;
import org.hsqldb.lib.LongLookup;

/**
 * Manages allocation of space for rows.<p>
 * Maintains a list of free file blocks with fixed capacity.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.1
 * @since 2.3.0
 */
public class TableSpaceManagerBlocks implements TableSpaceManager {

    //
    final DataSpaceManager spaceManager;
    final int              scale;
    final int              fileBlockSize;
    final int              spaceID;

    //
    private DoubleIntIndex  spaceList;
    private DoubleIntIndex  oldList;
    private DoubleLongIndex oldLargeList;
    private final int       capacity;
    private long            requestGetCount;
    private long            releaseCount;
    private long            requestCount;
    private long            requestSize;
    boolean                 isModified;
    boolean                 isInitialised;

    //
    long freshBlockFreePos = 0;
    long freshBlockLimit   = 0;
    long currentBlockFloor = 0;
    long currentBlockLimit = 0;
    int  fileBlockIndex    = -1;

    /**
     *
     */
    public TableSpaceManagerBlocks(DataSpaceManager spaceManager, int spaceId,
                                   int fileBlockSize, int capacity,
                                   int fileScale) {

        this.spaceManager  = spaceManager;
        this.spaceID       = spaceId;
        this.fileBlockSize = fileBlockSize;
        this.capacity      = capacity;
        this.scale         = fileScale;
        this.spaceList     = new DoubleIntIndex(capacity, true);

        spaceList.setValuesSearchTarget();

        oldList = new DoubleIntIndex(capacity, true);
    }

    public boolean hasFileRoom(long blockSize) {
        return freshBlockLimit - freshBlockFreePos > blockSize;
    }

    public void addFileBlock(long blockFreePos, long blockLimit) {

        int released = (int) (freshBlockLimit - freshBlockFreePos);

        if (released > 0) {
            release(freshBlockFreePos / scale, released);
        }

        initialiseFileBlock(null, blockFreePos, blockLimit);
    }

    public void initialiseFileBlock(LongLookup spaceList, long blockFreePos,
                                    long blockLimit) {

        isInitialised     = true;
        freshBlockFreePos = blockFreePos;
        freshBlockLimit   = blockLimit;
        currentBlockFloor = (freshBlockFreePos / fileBlockSize)
                            * (fileBlockSize / scale);
        currentBlockLimit = freshBlockLimit / scale;

        if (spaceList != null) {
            ((DoubleIntIndex) spaceList).copyTo(this.spaceList);
        }
    }

    private boolean getNewMainBlock(long rowSize) {

        long blockCount;
        long blockSize;
        long position;

        // called only on initialisation
        if (!isInitialised) {
            isInitialised = true;

            spaceManager.initialiseTableSpace(this);

            if (freshBlockFreePos + rowSize <= freshBlockLimit) {
                return true;
            }
        }

        blockCount = (fileBlockSize + rowSize) / fileBlockSize;
        blockSize  = blockCount * fileBlockSize;
        position   = spaceManager.getFileBlocks(spaceID, (int) blockCount);

        if (position < 0) {
            return false;
        }

        if (position != freshBlockLimit) {
            long released = freshBlockLimit - freshBlockFreePos;

            if (released > 0) {
                release(freshBlockFreePos / scale, (int) released);
            }

            freshBlockFreePos = position;
            freshBlockLimit   = position;
        }

        freshBlockLimit += blockSize;
        currentBlockFloor = (freshBlockFreePos / fileBlockSize)
                            * (fileBlockSize / scale);
        currentBlockLimit = freshBlockLimit / scale;

        if (oldList.size() + spaceList.size() > oldList.capacity()) {
            resetOldList();
        }

        oldList.addUnsorted(spaceList);
        resetOldList();
        spaceList.clear();

        return true;
    }

    private long getNewBlock(long rowSize) {

        if (freshBlockFreePos + rowSize > freshBlockLimit) {
            boolean result = getNewMainBlock(rowSize);

            if (!result) {
                throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
            }
        }

        long position = freshBlockFreePos;

        freshBlockFreePos += rowSize;

        return position / scale;
    }

    public int getSpaceID() {
        return spaceID;
    }

    synchronized public void release(long pos, int rowSize) {

        int rowUnits = rowSize / scale;

        isModified = true;

        releaseCount++;

        if (pos + rowUnits >= Integer.MAX_VALUE) {
            if (oldLargeList == null) {
                oldLargeList = new DoubleLongIndex(capacity);
            }

            oldLargeList.addUnsorted(pos, rowUnits);

            if (oldLargeList.size() == capacity) {
                resetOldList();
            }

            return;
        }

        if ((pos >= currentBlockFloor && pos < currentBlockLimit)) {
            spaceList.add(pos, rowUnits);

            if (spaceList.size() == capacity) {
                resetList(false);
            }
        } else {
            oldList.addUnsorted(pos, rowUnits);

            if (oldList.size() == capacity) {
                resetOldList();
            }
        }
    }

    /**
     * Returns the position of a free block or throws
     */
    synchronized public long getFilePosition(int rowSize) {

        requestGetCount++;

        if (capacity == 0) {
            return getNewBlock(rowSize);
        }

        int index    = -1;
        int rowUnits = rowSize / scale;

        if (spaceList.size() > 0) {
            if (spaceList.getValue(0) >= rowUnits) {
                index = 0;
            } else {
                index = spaceList.findFirstGreaterEqualKeyIndex(rowUnits);

                if (index == -1) {
                    spaceList.compactLookupAsIntervals();
                    spaceList.setValuesSearchTarget();

                    index = spaceList.findFirstGreaterEqualKeyIndex(rowUnits);
                }
            }
        }

        if (index == -1) {
            return getNewBlock(rowSize);
        }

        requestCount++;

        requestSize += rowSize;

        int key        = spaceList.getKey(index);
        int units      = spaceList.getValue(index);
        int difference = units - rowUnits;

        spaceList.remove(index);

        if (difference > 0) {
            int pos = key + rowUnits;

            spaceList.add(pos, difference);
        }

        return key;
    }

    public void reset() {

        if (freshBlockFreePos == 0) {
            fileBlockIndex = -1;
        } else {
            fileBlockIndex = (int) (freshBlockFreePos / fileBlockSize);
        }

        resetOldList();
        resetList(true);

        freshBlockFreePos = 0;
        freshBlockLimit   = 0;
        currentBlockFloor = 0;
        currentBlockLimit = 0;
    }

    public long getLostBlocksSize() {

        long total = freshBlockLimit - freshBlockFreePos
                     + spaceList.getTotalValues() * scale
                     + oldList.getTotalValues() * scale;

        return total;
    }

    public boolean isDefaultSpace() {
        return spaceID == DataSpaceManager.tableIdDefault;
    }

    public int getFileBlockIndex() {
        return fileBlockIndex;
    }

    private void resetList(boolean full) {

        spaceList.compactLookupAsIntervals();

        if (full) {
            spaceManager.freeTableSpace(spaceID, spaceList, freshBlockFreePos,
                                        freshBlockLimit);
            spaceList.clear();
            spaceList.setValuesSearchTarget();
        } else {
            if (spaceList.size() > capacity - 32) {
                int limit = capacity / 2;

                for (int i = 0; i < limit; i++) {
                    int pos      = spaceList.getKey(i);
                    int rowUnits = spaceList.getValue(i);

                    oldList.addUnsorted(pos, rowUnits);

                    if (oldList.size() == capacity) {
                        resetOldList();
                    }
                }

                spaceList.removeRange(0, limit);
                resetOldList();
            }

            spaceList.setValuesSearchTarget();
        }
    }

    private void resetOldList() {

        if (oldList.size() > 0) {
            oldList.compactLookupAsIntervals();
            spaceManager.freeTableSpace(spaceID, oldList, 0, 0);
            oldList.clear();
        }

        if (oldLargeList != null && oldLargeList.size() > 0) {
            oldLargeList.compactLookupAsIntervals();
            spaceManager.freeTableSpace(spaceID, oldLargeList, 0, 0);
            oldLargeList.clear();
        }
    }
}
