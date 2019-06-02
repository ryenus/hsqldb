/* Copyright (c) 2001-2019, The HSQL Development Group
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

import java.util.concurrent.atomic.AtomicInteger;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.DoubleIntIndex;
import org.hsqldb.lib.IntIndex;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedIntHashSet;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.4.1
 * @since 2.3.0
 */
public class DataSpaceManagerBlocks implements DataSpaceManager {

    //
    final DataFileCache           cache;
    final TableSpaceManagerBlocks defaultSpaceManager;
    final TableSpaceManagerBlocks directorySpaceManager;

    //
    final IntKeyHashMap spaceManagerList;

    //
    final BlockObjectStore rootStore;
    final BlockObjectStore directoryStore;
    final BlockObjectStore bitMapStore;

    //
    IntArrayCachedObject rootBlock;

    //
    final AtomicInteger spaceIdSequence = new AtomicInteger(tableIdFirst);
    final IntIndex      emptySpaceList;
    int                 released = 0;

    //
    public static final int dirBlockSize            = 1024 * 2;
    public static final int fileBlockItemCountLimit = 64 * 1024;

    //
    final int bitmapIntSize;
    final int bitmapStorageSize;
    final int fileBlockItemCount;
    final int fileBlockSize;
    final int dataFileScale;

    //
    BlockAccessor ba;

    public DataSpaceManagerBlocks(DataFileCache dataFileCache) {

        int bitmapStoreSizeTemp;

        cache         = dataFileCache;
        dataFileScale = cache.getDataFileScale();
        fileBlockSize = cache.database.logger.getDataFileSpaces() * 1024
                        * 1024;
        fileBlockItemCount = fileBlockSize / dataFileScale;
        bitmapIntSize      = fileBlockItemCount / Integer.SIZE;
        bitmapStoreSizeTemp = BitMapCachedObject.fileSizeFactor
                              * bitmapIntSize;

        if (bitmapStoreSizeTemp < DataSpaceManager.fixedBlockSizeUnit) {
            bitmapStoreSizeTemp = DataSpaceManager.fixedBlockSizeUnit;
        }

        bitmapStorageSize = bitmapStoreSizeTemp;
        ba                = new BlockAccessor();
        spaceManagerList  = new IntKeyHashMap();
        emptySpaceList    = new IntIndex(32, false);

        //
        directorySpaceManager = new TableSpaceManagerBlocks(this,
                tableIdDirectory, fileBlockSize, 16, dataFileScale, 0);
        defaultSpaceManager = new TableSpaceManagerBlocks(this,
                tableIdDefault, fileBlockSize,
                cache.database.logger.propMaxFreeBlocks, dataFileScale,
                cache.database.logger.propMinReuse);

        spaceManagerList.put(tableIdDirectory, directorySpaceManager);
        spaceManagerList.put(tableIdDefault, defaultSpaceManager);

        //
        rootStore      = getRootStore();
        directoryStore = getDirectoryStore(false);
        bitMapStore    = getBitMapStore();

        if (cache.spaceManagerPosition == 0) {
            initialiseNewSpaceDirectory();

            cache.spaceManagerPosition = rootBlock.getPos() * dataFileScale;
        } else {
            long pos = cache.spaceManagerPosition / dataFileScale;

            rootBlock = (IntArrayCachedObject) rootStore.get(pos, true);

            // integrity check
            if (getBlockIndexLimit() == 0) {
                throw Error.error(ErrorCode.FILE_IO_ERROR);
            }

            if (cache.isDataReadOnly()) {
                return;
            }

            initialiseSpaceList();
            initialiseTableSpace(directorySpaceManager);
            initialiseTableSpace(defaultSpaceManager);
        }
    }

    BlockObjectStore getRootStore() {

        return new BlockObjectStore(cache, directorySpaceManager,
                                    IntArrayCachedObject.class,
                                    IntArrayCachedObject.fileSizeFactor
                                    * dirBlockSize, dirBlockSize);
    }

    BlockObjectStore getDirectoryStore(boolean force240) {

        return new BlockObjectStore(
            cache, directorySpaceManager,
            DirectoryBlockCachedObject.class,
            DirectoryBlockCachedObject.fileSizeFactor * dirBlockSize,
            dirBlockSize);
    }

    BlockObjectStore getBitMapStore() {

        return new BlockObjectStore(cache, directorySpaceManager,
                                    BitMapCachedObject.class,
                                    bitmapStorageSize, bitmapIntSize);
    }
    private void initialiseNewSpaceDirectory() {

        long currentSize = cache.getFileFreePos();
        long totalBlocks = (currentSize / fileBlockSize) + 1;
        long lastFreePosition = cache.enlargeFileSpace(totalBlocks
            * fileBlockSize - currentSize);

        defaultSpaceManager.initialiseFileBlock(null, lastFreePosition,
                cache.getFileFreePos());

        long defaultSpaceBlockCount = totalBlocks;
        long directorySpaceBlockCount =
            calculateDirectorySpaceBlocks(totalBlocks);

        lastFreePosition = cache.enlargeFileSpace(directorySpaceBlockCount
                * fileBlockSize);

        // file block is empty
        directorySpaceManager.initialiseFileBlock(null, lastFreePosition,
                cache.getFileFreePos());

        IntArrayCachedObject root = new IntArrayCachedObject(dirBlockSize);

        rootStore.add(root, true);

        rootBlock = root;

        createFileBlocksInDirectory((int) defaultSpaceBlockCount,
                                    (int) directorySpaceBlockCount,
                                    tableIdDirectory);
        createFileBlocksInDirectory(0, (int) defaultSpaceBlockCount,
                                    tableIdDefault);
    }

    private long calculateDirectorySpaceBlocks(long blockCount) {

        long currentSize   = calculateDirectorySpaceSize(blockCount);
        long currentBlocks = currentSize / fileBlockSize + 1;

        currentSize   += calculateDirectorySpaceSize(currentBlocks);
        currentBlocks = currentSize / fileBlockSize + 1;

        return currentBlocks;
    }

    private long calculateDirectorySpaceSize(long blockCount) {

        long blockLimit = ArrayUtil.getBinaryMultipleCeiling(blockCount + 1,
            dirBlockSize);
        long currentSize = IntArrayCachedObject.fileSizeFactor * blockLimit;    // root

        currentSize += DirectoryBlockCachedObject.fileSizeFactor * blockLimit;    // directory
        currentSize += bitmapStorageSize * (blockCount + 1);                      // bitmaps

        return currentSize;
    }

    /**
     * The space for a new directory block must be added to the directorySpaceManager
     * before createFileBlocksInDirectory is called, otherwise there is no space
     * to create the bit-map
     */
    private void ensureDirectorySpaceAvailable(int blockCount) {

        int dirObjectSize = bitmapStorageSize * blockCount;

        dirObjectSize += DirectoryBlockCachedObject.fileSizeFactor
                         * dirBlockSize;

        boolean hasRoom = directorySpaceManager.hasFileRoom(dirObjectSize);

        if (!hasRoom) {
            long cacheFreePos;
            int  index         = getBlockIndexLimit();
            int  dirBlockCount = dirObjectSize / fileBlockSize + 1;
            long filePosition = cache.enlargeFileSpace((long) dirBlockCount
                * fileBlockSize);

            directorySpaceManager.addFileBlock(filePosition,
                                               filePosition
                                               + (long) dirBlockCount
                                                 * fileBlockSize);
            createFileBlocksInDirectory(index, dirBlockCount,
                                        tableIdDirectory);

            // integrity check
            cacheFreePos = cache.getFileFreePos();
            index        = getBlockIndexLimit();

            if ((long) index * fileBlockSize != cacheFreePos) {
                cache.logSevereEvent(
                    "space manager end file pos different from data file: "
                    + (index * fileBlockSize) + ", " + cacheFreePos, null);
            }
        }
    }

    /**
     * try available blocks first, then get fresh block
     */
    public long getFileBlocks(int tableId, int blockCount) {

        cache.writeLock.lock();

        try {
            long index = getExistingBlockIndex(tableId, blockCount);

            if (index > 0) {
                return index * fileBlockSize;
            } else {
                return getNewFileBlocks(tableId, blockCount);
            }
        } finally {
            cache.writeLock.unlock();
        }
    }

    private long getNewFileBlocks(int tableId, int blockCount) {

        ensureDirectorySpaceAvailable(blockCount);

        return getNewFileBlocksNoCheck(tableId, blockCount);
    }

    private long getNewFileBlocksNoCheck(int tableId, int blockCount) {

        long index        = getBlockIndexLimit();
        long filePosition = index * fileBlockSize;
        long delta = filePosition + ((long) blockCount * fileBlockSize)
                     - cache.getFileFreePos();

        if (delta > 0) {
            cache.enlargeFileSpace(delta);
        }

        createFileBlocksInDirectory((int) index, blockCount, tableId);

        return filePosition;
    }

    private void createFileBlocksInDirectory(int fileBlockIndex,
            int blockCount, int tableId) {

        for (int i = 0; i < blockCount; i++) {
            createFileBlockInDirectory(fileBlockIndex + i, tableId);
        }
    }

    private void createFileBlockInDirectory(int fileBlockIndex, int tableId) {

        BitMapCachedObject bitMap = new BitMapCachedObject(bitmapIntSize);

        bitMapStore.add(bitMap, false);

        //
        int bitmapBlockPos = (int) (bitMap.getPos() * dataFileScale
                                    / DataSpaceManager.fixedBlockSizeUnit);
        int blockOffset = fileBlockIndex % dirBlockSize;
        DirectoryBlockCachedObject directory = getDirectory(fileBlockIndex,
            true);

        if (directory == null) {
            createDirectory(fileBlockIndex);

            directory = getDirectory(fileBlockIndex, true);
        }

        directory.setTableId(blockOffset, tableId);
        directory.setBitmapAddress(blockOffset, bitmapBlockPos);
        directory.keepInMemory(false);
    }

    private DirectoryBlockCachedObject getDirectory(int fileBlockIndex,
            boolean keep) {

        int indexInRoot = fileBlockIndex / dirBlockSize;

        return getDirectoryByIndex(indexInRoot, keep);
    }

    private DirectoryBlockCachedObject getDirectoryByIndex(int indexInRoot,
            boolean keep) {

        long                       position = rootBlock.getValue(indexInRoot);
        DirectoryBlockCachedObject directory;

        if (position == 0) {
            return null;
        }

        position *= (DataSpaceManager.fixedBlockSizeUnit / dataFileScale);
        directory = (DirectoryBlockCachedObject) directoryStore.get(position,
                keep);

        return directory;
    }

    private void createDirectory(int fileBlockIndex) {

        DirectoryBlockCachedObject directory;

        directory = new DirectoryBlockCachedObject(dirBlockSize);

        directoryStore.add(directory, false);

        int indexInRoot = fileBlockIndex / dirBlockSize;
        int blockPosition = (int) (directory.getPos() * dataFileScale
                                   / DataSpaceManager.fixedBlockSizeUnit);

        rootBlock.setValue(indexInRoot, blockPosition);
    }

    private int getBlockIndexLimit() {

        int indexInRoot = rootBlock.getNonZeroSize();

        if (indexInRoot == 0) {
            return 0;
        }

        indexInRoot--;

        int directoryBlockOffset = getDirectoryIndexLimit(indexInRoot);

        return indexInRoot * dirBlockSize + directoryBlockOffset;
    }

    private int getDirectoryIndexLimit(int indexInRoot) {

        DirectoryBlockCachedObject directory = getDirectoryByIndex(indexInRoot,
            false);
        int[] bitmapArray = directory.getBitmapAddressArray();
        int   index       = 0;

        for (; index < bitmapArray.length; index++) {
            if (bitmapArray[index] == 0) {
                break;
            }
        }

        return index;
    }

    private void initialiseSpaceList() {

        int               maxId = tableIdDefault;
        OrderedIntHashSet list  = new OrderedIntHashSet();

        ba.initialise(false);

        try {
            for (;;) {
                boolean result = ba.nextBlock();

                if (!result) {
                    break;
                }

                int currentId = ba.getTableId();

                if (currentId > maxId) {
                    maxId = currentId;
                }

                if (currentId == tableIdEmpty) {
                    int freeItems    = ba.getFreeSpaceValue();
                    int freeItemsEnd = ba.getFreeBlockValue();

                    if (freeItems == 0 && freeItemsEnd == 0) {
                        emptySpaceList.addUnique(ba.currentBlockIndex);
                    } else {
                        list.add(ba.currentBlockIndex);
                    }
                }
            }
        } finally {
            ba.reset();
        }

        spaceIdSequence.set((maxId + 2) & -2);

        if (list.size() > 0) {
            setAsideBlocks(list);

            String s =
                "space manager error - recovered (freeItems in empty blocks) : ("
                + list.size() + ")";

            cache.logSevereEvent(s, null);
        }
    }

    private int getExistingBlockIndex(int tableId, int blockCount) {

        int blockIndex = emptySpaceList.removeFirstConsecutiveKeys(blockCount,
            -1);

        if (blockIndex > 0) {
            setDirectoryBlocksAsTable(tableId, blockIndex, blockCount);
        }

        return blockIndex;
    }

    /**
     * index and blockCount always valid
     */
    private void setDirectoryBlocksAsTable(int tableId, int blockIndex,
                                           int blockCount) {

        int                        directoryIndex = -1;
        DirectoryBlockCachedObject directory      = null;

        for (int i = blockIndex; i < blockIndex + blockCount; i++) {
            if (directoryIndex != i / dirBlockSize) {
                if (directory != null) {
                    directory.keepInMemory(false);
                }

                directory      = getDirectory(i, true);
                directoryIndex = i / dirBlockSize;
            }

            int offset = i % dirBlockSize;

            directory.setTableId(offset, tableId);
        }

        directory.keepInMemory(false);
    }

    public TableSpaceManager getDefaultTableSpace() {
        return defaultSpaceManager;
    }

    public TableSpaceManager getTableSpace(int spaceId) {

        if (spaceId == DataSpaceManager.tableIdDefault) {
            return defaultSpaceManager;
        }

        if (spaceId >= spaceIdSequence.get()) {
            spaceIdSequence.set((spaceId + 2) & -2);
        }

        cache.writeLock.lock();

        try {
            TableSpaceManagerBlocks manager =
                (TableSpaceManagerBlocks) spaceManagerList.get(spaceId);

            if (manager == null) {
                int minReuse = cache.database.logger.propMinReuse;

                manager = new TableSpaceManagerBlocks(
                    this, spaceId, fileBlockSize,
                    cache.database.logger.propMaxFreeBlocks, dataFileScale,
                    minReuse);

                spaceManagerList.put(spaceId, manager);
            }

            return manager;
        } finally {
            cache.writeLock.unlock();
        }
    }

    public int getNewTableSpaceID() {
        return spaceIdSequence.getAndAdd(2);
    }

    public void freeTableSpace(int spaceId) {

        if (spaceId == tableIdDefault || spaceId == tableIdDirectory) {
            return;
        }

        cache.writeLock.lock();

        try {
            TableSpaceManager tableSpace =
                (TableSpaceManager) spaceManagerList.get(spaceId);

            if (tableSpace != null) {
                tableSpace.reset();
                // do not remove from spaceManagerList - can be truncate
            }

            IntIndex list = new IntIndex(16, false);

            ba.initialise(true);

            try {
                while (ba.nextBlockForTable(spaceId)) {
                    list.addUnsorted(ba.currentBlockIndex);
                    ba.setTable(tableIdEmpty);
                    emptySpaceList.addUnique(ba.currentBlockIndex);
                }
            } finally {
                ba.reset();
            }

            cache.releaseRange(list, fileBlockItemCount);
        } finally {
            cache.writeLock.unlock();
        }
    }

    public void freeTableSpace(int spaceId, DoubleIntIndex spaceList,
                               long offset, long limit, boolean full) {

        if (spaceList.size() == 0 && offset == limit) {
            return;
        }

        // sorts by keys
        spaceList.compactLookupAsIntervals();

        if (!full) {
            int available = spaceList.capacity() - spaceList.size();

            if (available > spaceList.capacity() / 4) {
                spaceList.setValuesSearchTarget();
                spaceList.sort();

                return;
            }
        }

        cache.writeLock.lock();

        try {
            ba.initialise(true);

            try {

                // spaceId may be the tableIdDefault for moved spaces
                int[] keys   = spaceList.getKeys();
                int[] values = spaceList.getValues();

                for (int i = 0; i < spaceList.size(); i++) {
                    int position = keys[i];
                    int units    = values[i];

                    freeTableSpacePart(position, units);
                }

                long position = offset / dataFileScale;
                int  units    = (int) ((limit - offset) / dataFileScale);

                freeTableSpacePart(position, units);
            } finally {
                ba.reset();
            }
        } finally {
            cache.writeLock.unlock();
        }

        spaceList.clear();
        spaceList.setValuesSearchTarget();
    }

    private void freeTableSpacePart(long position, int units) {

        for (; units > 0; ) {

            // count can cover more than one file block
            int blockIndex   = (int) (position / fileBlockItemCount);
            int offset       = (int) (position % fileBlockItemCount);
            int currentUnits = fileBlockItemCount - offset;

            if (currentUnits > units) {
                currentUnits = units;
            }

            boolean result = ba.moveToBlock(blockIndex);

            if (result) {
                int setCount = ba.setRange(offset, currentUnits);

                if (setCount != currentUnits) {
                    ba.unsetRange(offset, currentUnits);

                    String s =
                        "space manager error - recovered (block, offset, units) : ("
                        + blockIndex + "," + offset + "," + units + ")";

                    cache.logSevereEvent(s, null);
                }
            } else {
                String s =
                    "space manager error - recovered (block, offset, units) : ("
                    + blockIndex + "," + offset + "," + units + ")";

                cache.logSevereEvent(s, null);
            }

            units    -= currentUnits;
            position += currentUnits;
        }
    }

    /**
     * Returns space id
     *
     * returns - 1 if pointer is beyond the last allocated block
     */
    int findTableSpace(long position) {

        int blockIndex = (int) (position / fileBlockItemCount);

        cache.writeLock.lock();

        try {
            ba.initialise(false);

            try {
                boolean result = ba.moveToBlock(blockIndex);

                if (!result) {
                    return -1;
                }

                int id = ba.getTableId();

                return id;
            } finally {
                ba.reset();
            }
        } finally {
            cache.writeLock.unlock();
        }
    }

    void setAsideBlocks(OrderedIntHashSet blocks) {

        cache.writeLock.lock();

        try {
            ba.initialise(true);

            try {
                for (int i = 0; i < blocks.size(); i++) {
                    int     block  = blocks.get(i);
                    boolean result = ba.moveToBlock(block);

                    if (result) {
                        ba.setTable(DataSpaceManager.tableIdSetAside);
                    }
                }
            } finally {
                ba.reset();
            }
        } finally {
            cache.writeLock.unlock();
        }
    }

    public long getLostBlocksSize() {

        long fragment = 0;

        cache.writeLock.lock();

        try {
            ba.initialise(false);

            try {
                for (;;) {
                    boolean result = ba.nextBlock();

                    if (!result) {
                        break;
                    }

                    if (ba.getTableId() == tableIdDirectory) {
                        continue;
                    }

                    fragment += ba.getFreeSpaceValue() * dataFileScale;

                    if (ba.getTableId() == tableIdEmpty) {
                        fragment += fileBlockSize;
                    }
                }
            } finally {
                ba.reset();
            }
        } finally {
            cache.writeLock.unlock();
        }

        return fragment;
    }

    public int getFileBlockSize() {
        return fileBlockSize;
    }

    public boolean isModified() {
        return true;
    }

    public void initialiseSpaces() {

        cache.writeLock.lock();

        try {
            Iterator it = spaceManagerList.values().iterator();

            while (it.hasNext()) {
                TableSpaceManagerBlocks tableSpace =
                    (TableSpaceManagerBlocks) it.next();

                if (tableSpace.getSpaceID() == DataSpaceManager
                        .tableIdDirectory || tableSpace
                        .getFileBlockIndex() != -1) {
                    initialiseTableSpace(tableSpace);
                }
            }
        } finally {
            cache.writeLock.unlock();
        }
    }

    public void reset() {

        cache.writeLock.lock();

        try {
            Iterator it = spaceManagerList.values().iterator();

            while (it.hasNext()) {
                TableSpaceManagerBlocks tableSpace =
                    (TableSpaceManagerBlocks) it.next();

                tableSpace.reset();
            }
        } finally {
            cache.writeLock.unlock();
        }
    }

    public boolean isMultiSpace() {
        return true;
    }

    public int getFileBlockItemCount() {
        return fileBlockItemCount;
    }

    public DirectoryBlockCachedObject[] getDirectoryList() {

        int                          count = 0;
        DirectoryBlockCachedObject[] directoryList;
        int[]                        rootArray = rootBlock.getIntArray();

        while (rootArray[count] != 0) {
            count++;
        }

        directoryList = new DirectoryBlockCachedObject[count];

        for (int i = 0; i < directoryList.length; i++) {
            directoryList[i] = getDirectory(i * dirBlockSize, false);
        }

        return directoryList;
    }

    /**
     * return keys are file block indexes, values are bad (off) space ids
     * or tableIdDirectory when two bitmpas have the same pointer
     */
    DoubleIntIndex checkDirectorySpaces() {

        DirectoryBlockCachedObject[] directoryList = getDirectoryList();
        DoubleIntIndex offspaceBitmaps = new DoubleIntIndex(8, false);

        offspaceBitmaps.setKeysSearchTarget();

        DoubleIntIndex positionBitmaps = new DoubleIntIndex(8, false);

        positionBitmaps.setKeysSearchTarget();

        for (int i = 0; i < directoryList.length; i++) {
            DirectoryBlockCachedObject dir        = directoryList[i];
            long                       position   = dir.getPos();
            int                        spaceId    = findTableSpace(position);
            int                        blockIndex = i;
            int blockPos = rootBlock.getValue(blockIndex);
            boolean                    result;
            int count = dir.getStorageSize()
                        / DataSpaceManager.fixedBlockSizeUnit;

            for (int j = 0; j < count; j++) {
                result = positionBitmaps.addUnique(blockPos, blockIndex);
            }

            int[] bitMapAddress = dir.getBitmapAddressArray();

            for (int j = 0; j < bitMapAddress.length; j++) {
                blockPos = dir.getBitmapAddress(j);

                if (blockPos == 0) {
                    break;
                }

                position = blockPos
                           * (DataSpaceManager.fixedBlockSizeUnit
                              / dataFileScale);
                spaceId    = findTableSpace(position);
                blockIndex = i * dirBlockSize + j;

                if (spaceId != DataSpaceManager.tableIdDirectory) {
                    offspaceBitmaps.add(blockIndex, spaceId);
                } else {
                    result = positionBitmaps.addUnique(blockPos, blockIndex);

                    if (!result) {
                        offspaceBitmaps.add(blockIndex, spaceId);

                        int offset =
                            positionBitmaps.findFirstEqualKeyIndex(blockPos);

                        blockIndex = positionBitmaps.getValue(offset);

                        offspaceBitmaps.add(blockIndex, spaceId);
                    }
                }
            }
        }

        return offspaceBitmaps;
    }

    /**
     * return keys are file block indexes, values are space ids
     */
    DoubleIntIndex checkDirectoryBitmaps(DirectoryBlockCachedObject mismatch) {

        DirectoryBlockCachedObject[] directoryList = getDirectoryList();
        DoubleIntIndex offspaceBitmaps = new DoubleIntIndex(8, false);

        offspaceBitmaps.setKeysSearchTarget();

        int mismatchCount = 0;

        for (int i = 0; i < directoryList.length; i++) {
            DirectoryBlockCachedObject dir = directoryList[i];
            int[] bitMapAddress            = dir.getBitmapAddressArray();

            for (int j = 0; j < bitMapAddress.length; j++) {
                int blockPos = dir.getBitmapAddress(j);

                if (blockPos == 0) {
                    break;
                }

                long position = blockPos
                                * (DataSpaceManager.fixedBlockSizeUnit
                                   / dataFileScale);
                int spaceId    = findTableSpace(position);
                int blockIndex = i * dirBlockSize + j;
                BitMapCachedObject currentBitMap =
                    (BitMapCachedObject) bitMapStore.get(position, false);

                spaceId = dir.getTableId(j);

                int freeUnits      = currentBitMap.bitMap.countSetBits();
                int freeBlockUnits = currentBitMap.bitMap.countSetBitsEnd();

                if (dir.getFreeSpace(j) != freeUnits
                        || dir.getFreeBlock(j) != freeBlockUnits) {
                    offspaceBitmaps.add(blockIndex, spaceId);
                    mismatch.setTableId(mismatchCount, spaceId);
                    mismatch.setFreeSpace(mismatchCount, (char) freeUnits);
                    mismatch.setFreeBlock(mismatchCount,
                                          (char) freeBlockUnits);

                    mismatchCount++;

                    if (mismatchCount == mismatch.getTableIdArray().length) {
                        break;
                    }
                }
            }
        }

        return offspaceBitmaps;
    }

    private void initialiseTableSpace(TableSpaceManagerBlocks tableSpace) {

        int spaceId        = tableSpace.getSpaceID();
        int blockIndex     = -1;
        int lastBlockIndex = tableSpace.getFileBlockIndex();

        if (lastBlockIndex >= 0) {
            if (hasFreeSpace(spaceId, lastBlockIndex)) {
                blockIndex = lastBlockIndex;
            }
        }

        if (blockIndex < 0) {
            blockIndex = findLargestFreeSpace(spaceId);
        }

        if (blockIndex < 0) {
            return;
        }

        initialiseTableSpace(tableSpace, blockIndex);
    }

    private boolean hasFreeSpace(int spaceId, int blockIndex) {

        ba.initialise(false);

        try {
            boolean result = ba.moveToBlock(blockIndex);

            if (result) {
                if (ba.getTableId() == spaceId) {
                    if (ba.getFreeBlockValue() > 0) {
                        return true;
                    }
                }
            }

            return false;
        } finally {
            ba.reset();
        }
    }

    private int findLargestFreeSpace(int spaceId) {

        int maxFree    = 0;
        int blockIndex = -1;

        ba.initialise(false);

        try {
            for (; ba.nextBlockForTable(spaceId); ) {

                // find the largest free
                int currentFree = ba.getFreeBlockValue();

                if (currentFree > maxFree) {
                    blockIndex = ba.currentBlockIndex;
                    maxFree    = currentFree;
                }
            }

            return blockIndex;
        } finally {
            ba.reset();
        }
    }

    private void initialiseTableSpace(TableSpaceManagerBlocks tableSpace,
                                      int blockIndex) {

        // get existing file block and initialise
        ba.initialise(true);

        try {
            ba.moveToBlock(blockIndex);

            int  freeItems = ba.getFreeBlockValue();
            long blockPos  = (long) blockIndex * fileBlockSize;
            int unsetCount = ba.unsetRange(fileBlockItemCount - freeItems,
                                           freeItems);

            if (unsetCount == freeItems) {
                tableSpace.initialiseFileBlock(
                    null,
                    blockPos + (fileBlockSize - freeItems * dataFileScale),
                    blockPos + fileBlockSize);
            } else {
                cache.logSevereEvent("space manager error - recovered", null);
            }
        } finally {
            ba.reset();
        }
    }

    private class BlockAccessor {

        boolean                    currentKeep;
        int                        currentBlockIndex  = -1;
        int                        currentDirIndex    = -1;
        int                        currentBlockOffset = -1;
        DirectoryBlockCachedObject currentDir         = null;
        BitMapCachedObject         currentBitMap      = null;

        void initialise(boolean forUpdate) {
            currentKeep = forUpdate;
        }

        boolean nextBlock() {

            boolean result = moveToBlock(currentBlockIndex + 1);

            return result;
        }

        boolean nextBlockForTable(int tableId) {

            for (;;) {
                boolean result = moveToBlock(currentBlockIndex + 1);

                if (!result) {
                    return false;
                }

                if (getTableId() == tableId) {
                    return true;
                }
            }
        }

        boolean moveToBlock(int fileBlockIndex) {

            if (currentBlockIndex != fileBlockIndex) {
                endBlockUpdate();

                currentBitMap = null;

                if (currentDirIndex != fileBlockIndex / dirBlockSize) {
                    reset();

                    currentDirIndex = fileBlockIndex / dirBlockSize;
                    currentDir = getDirectory(fileBlockIndex, currentKeep);
                }

                if (currentDir == null) {
                    reset();

                    return false;
                }

                currentBlockIndex  = fileBlockIndex;
                currentBlockOffset = fileBlockIndex % dirBlockSize;

                long position =
                    currentDir.getBitmapAddress(currentBlockOffset);

                if (position == 0) {
                    reset();

                    return false;
                }

                if (currentKeep) {
                    position *= (DataSpaceManager.fixedBlockSizeUnit
                                 / dataFileScale);
                    currentBitMap =
                        (BitMapCachedObject) bitMapStore.get(position, true);
                }
            }

            return true;
        }

        int setRange(int offset, int currentUnits) {

            currentBitMap.setChanged(true);

            return currentBitMap.bitMap.setRange(offset, currentUnits);
        }

        int unsetRange(int offset, int currentUnits) {

            currentBitMap.setChanged(true);

            return currentBitMap.bitMap.unsetRange(offset, currentUnits);
        }

        void reset() {

            endBlockUpdate();

            if (currentDir != null) {
                if (currentKeep) {
                    currentDir.keepInMemory(false);
                }
            }

            currentBlockIndex  = -1;
            currentDirIndex    = -1;
            currentBlockOffset = -1;
            currentDir         = null;
            currentBitMap      = null;
        }

        private void endBlockUpdate() {

            if (currentBitMap == null) {
                return;
            }

            if (!currentBitMap.hasChanged()) {
                currentBitMap.keepInMemory(false);

                return;
            }

            int freeUnits      = currentBitMap.bitMap.countSetBits();
            int freeBlockUnits = currentBitMap.bitMap.countSetBitsEnd();

            if (freeUnits == fileBlockItemCount) {
                int currentId =
                    currentDir.getTableIdArray()[currentBlockOffset];

                if (currentId != DataSpaceManager.tableIdSetAside) {
                    setTable(DataSpaceManager.tableIdEmpty);
                    emptySpaceList.addUnique(currentBlockIndex);

                    released++;
                }

                currentBitMap.keepInMemory(false);

                return;
            }

            currentBitMap.keepInMemory(false);
            currentDir.setFreeSpace(currentBlockOffset, (char) freeUnits);
            currentDir.setFreeBlock(currentBlockOffset, (char) freeBlockUnits);
        }

        void setTable(int tableId) {

            currentDir.setTableId(currentBlockOffset, tableId);
            currentDir.setFreeSpace(currentBlockOffset, (char) 0);
            currentDir.setFreeBlock(currentBlockOffset, (char) 0);
            currentBitMap.bitMap.reset();
            currentBitMap.setChanged(true);
        }

        int getTableId() {
            return currentDir.getTableId(currentBlockOffset);
        }

        char getFreeSpaceValue() {
            return currentDir.getFreeSpace(currentBlockOffset);
        }

        char getFreeBlockValue() {
            return currentDir.getFreeBlock(currentBlockOffset);
        }
    }
}
