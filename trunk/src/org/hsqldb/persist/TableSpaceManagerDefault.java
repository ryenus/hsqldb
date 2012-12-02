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

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.DoubleIntIndex;
import org.hsqldb.lib.ArrayUtil;

/**
 * Maintains a list of free file blocks.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.8.0
 */
public class TableSpaceManagerDefault implements TableSpaceManager {

    private DataFileCache  cache;
    private final int      scale;
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

    /**
     *
     */
    public TableSpaceManagerDefault(DataFileCache cache, int capacity,
                                    long lostSize) {

        this.cache = cache;
        lookup     = new DoubleIntIndex(capacity, true);

        lookup.setValuesSearchTarget();

        this.capacity          = capacity;
        this.scale             = cache.getDataFileScale();
        this.lostFreeBlockSize = lostSize;
        this.midSize           = 128;    // arbitrary initial value
    }

    public int getSpaceID() {
        return DataSpaceManager.tableIdDefault;
    }

    /**
     */
    public void release(long pos, int rowSize) {

        isModified = true;

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
            lookup.add(pos, rowSize);

            freeBlockSize += rowSize;
        }
    }

    long getNewBlock(long rowSize, boolean asBlocks) {

        cache.writeLock.lock();

        try {
            long i;
            long position;
            long newFreePosition;

            position = cache.getFileFreePos();

            if (asBlocks) {
                position = ArrayUtil.getBinaryMultipleCeiling(position,
                        DataSpaceManager.fixedBlockSizeUnit);
            }

            newFreePosition = position + rowSize;

            if (newFreePosition > cache.maxDataFileSize) {
                cache.logSevereEvent("data file reached maximum size "
                                     + cache.dataFileName, null);

                throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
            }

            boolean result = cache.dataFile.ensureLength(newFreePosition);

            if (!result) {
                cache.logSevereEvent(
                    "data file cannot be enlarged - disk spacee "
                    + cache.dataFileName, null);

                throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
            }

            cache.fileFreePosition = newFreePosition;

            return position / scale;
        } finally {
            cache.writeLock.unlock();
        }
    }

    /**
     * Returns the position of a free block or 0.
     */
    public long getFilePosition(long rowSize, boolean asBlocks) {

        cache.writeLock.lock();

        try {
            if (capacity == 0) {
                return getNewBlock(rowSize, asBlocks);
            }

            if (asBlocks) {
                rowSize = ArrayUtil.getBinaryMultipleCeiling(rowSize,
                        DataSpaceManager.fixedBlockSizeUnit);
            }

        if (rowSize > Integer.MAX_VALUE) {
            return getNewBlock(rowSize, asBlocks);
        }

            int index = lookup.findFirstGreaterEqualKeyIndex((int) rowSize);

            if (index == -1) {
                return getNewBlock(rowSize, asBlocks);
            }

            if (asBlocks) {
                for (; index < lookup.size(); index++) {
                    if (lookup.getValue(index)
                            % (DataSpaceManager.fixedBlockSizeUnit
                               / scale) == 0) {
                        break;
                    }
                }

                if (index == lookup.size()) {
                    return getNewBlock(rowSize, asBlocks);
                }
            }

            // statistics for successful requests only - to be used later for midSize
            requestCount++;

            requestSize += rowSize;

            int length     = lookup.getValue(index);
            int difference = length - (int) rowSize;
            int key        = lookup.getKey(index);

            lookup.remove(index);

            freeBlockSize -= rowSize;

            if (difference >= midSize) {
                long pos = key + (rowSize / scale);

                lookup.add(pos, difference);
            } else {
                lostFreeBlockSize += difference;
                freeBlockSize     -= difference;
            }

            return key;
        } finally {
            cache.writeLock.unlock();
        }
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

    public boolean hasFileRoom(int blockSize) {

        long newFreePosition = cache.getFileFreePos() + blockSize;

        return cache.dataFile.ensureLength(newFreePosition);
    }

    public void addFileBlock(long blockPos, long blockFreePos,
                             long blockLimit) {}

    public void initialiseFileBlock(long blockPos, long blockFreePos,
                                    long blockLimit) {}

    private void resetList() {

        if (requestCount != 0) {
            midSize = (int) (requestSize / requestCount);
        }

        int first = lookup.findFirstGreaterEqualSlotIndex(midSize);

        if (first < lookup.size() / 4) {
            first = lookup.size() / 4;
        }

        for (int i = 0; i < first; i++) {
            lostFreeBlockSize += lookup.getValue(i);
        }

        lookup.removeRange(0, first);
    }
}
