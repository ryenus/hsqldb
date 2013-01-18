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

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.3.0
 */
public class DataSpaceManagerSimple implements DataSpaceManager {

    DataFileCache     cache;
    TableSpaceManager defaultSpaceManager;
    int               fileBlockSize = DataSpaceManager.fixedBlockSizeUnit;
    long              totalFragmentSize;
    int               spaceIdSequence = tableIdFirst;

    /**
     * Used for readonly, Text and Session data files
     */
    DataSpaceManagerSimple(DataFileCache cache) {

        this.cache = cache;

        if (cache instanceof DataFileCacheSession) {
            defaultSpaceManager = new TableSpaceManagerSimple(cache);
        } else if (cache instanceof TextCache) {
            defaultSpaceManager = new TableSpaceManagerSimple(cache);
        } else {
            int capacity = cache.database.logger.propMaxFreeBlocks;

            defaultSpaceManager = new TableSpaceManagerBlocks(this,
                    DataSpaceManager.tableIdDefault, fileBlockSize, capacity,
                    cache.dataFileScale);

            initialiseTableSpace();
        }
    }

    public TableSpaceManager getDefaultTableSpace() {
        return defaultSpaceManager;
    }

    public TableSpaceManager getTableSpace(int spaceId) {

        if (spaceId >= spaceIdSequence) {
            spaceIdSequence = spaceId + 1;
        }

        return defaultSpaceManager;
    }

    public int getNewTableSpace() {
        return spaceIdSequence++;
    }

    public long getFileBlocks(int tableId, int blockCount) {

        long filePosition = cache.enlargeFileSpace(blockCount * fileBlockSize);

        return filePosition;
    }

    public void freeTableSpace(int spaceId) {}

    public void freeTableSpace(int spaceId, DoubleIntIndex spaceList) {

        for (int i = 0; i < spaceList.size(); i++) {
            totalFragmentSize += spaceList.getValue(i);
        }
    }

    public void freeTableSpace(int spaceId, long offset, long limit) {

        if (cache.fileFreePosition == limit) {
            cache.fileFreePosition = offset;
        }
    }

    public long getLostBlocksSize() {
        return totalFragmentSize;
    }

    public int getFileBlockSize() {
        return Integer.MAX_VALUE;
    }

    public boolean isModified() {
        return false;
    }

    public void close() {
        defaultSpaceManager.close();
    }

    public void reopen() {
        initialiseTableSpace();
    }

    private void initialiseTableSpace() {

        long currentSize = cache.getFileFreePos();
        long totalBlocks = (currentSize / fileBlockSize) + 1;
        long lastFreePosition = cache.enlargeFileSpace(totalBlocks
            * fileBlockSize - currentSize);

        defaultSpaceManager.initialiseFileBlock((totalBlocks - 1)
                * fileBlockSize, lastFreePosition, cache.getFileFreePos());
    }
}
