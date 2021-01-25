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

import java.util.concurrent.atomic.AtomicInteger;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.DoubleIntIndex;
import org.hsqldb.lib.IntIndex;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.LongLookup;
import org.hsqldb.lib.OrderedIntHashSet;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.1
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
    final BlockObjectStore lastBlockStore;

    //
    IntArrayCachedObject       rootBlock;
    DoubleIntArrayCachedObject lastBlocks;

    //
    final AtomicInteger spaceIdSequence = new AtomicInteger(tableIdFirst);
    final IntIndex      emptySpaceList;
    int                 released = 0;

    //
    public static final int lastBlockListSize       = 1024;
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

        cache              = dataFileCache;
        dataFileScale      = cache.getDataFileScale();
        fileBlockSize      = cache.getDataFileSpace() * 1024 * 1024;
        fileBlockItemCount = fileBlockSize / dataFileScale;
        bitmapIntSize      = fileBlockItemCount / Integer.SIZE;
        bitmapStoreSizeTemp = BitMapCachedObject.fileSizeFactor
                              * bitmapIntSize;

        if (bitmapStoreSizeTemp < fixedDiskBlockSize) {
            bitmapStoreSizeTemp = fixedDiskBlockSize;
        }

        bitmapStorageSize = bitmapStoreSizeTemp;
        ba                = new BlockAccessor();
        spaceManagerList  = new IntKeyHashMap();
        emptySpaceList    = new IntIndex(32, false);

        //
        directorySpaceManager = new TableSpaceManagerBlocks(this,
                tableIdDirectory, fileBlockSize, 16, dataFileScale);
        defaultSpaceManager = new TableSpaceManagerBlocks(this,
                tableIdDefault, fileBlockSize,
                cache.database.logger.propMaxFreeBlocks, dataFileScale);

        spaceManagerList.put(tableIdDirectory, directorySpaceManager);
        spaceManagerList.put(tableIdDefault, defaultSpaceManager);

        //
        rootStore      = getRootStore();
        directoryStore = getDirectoryStore();
        bitMapStore    = getBitMapStore();
        lastBlockStore = getLastBlockStore();

        if (cache.spaceManagerPosition == 0) {
            initialiseNewSpaceDirectory();

            cache.spaceManagerPosition = rootBlock.getPos() * dataFileScale;
        } else {
            long position = cache.spaceManagerPosition / dataFileScale;

            rootBlock = (IntArrayCachedObject) rootStore.get(position, true);

            // integrity check
            if (getBlockIndexLimit() == 0) {
                throw Error.error(ErrorCode.FILE_IO_ERROR);
            }

            if (cache.isDataReadOnly()) {
                return;
            }

            initialiseSpaceList();

            int blockPos = rootBlock.getValue(dirBlockSize - 1);

            if (blockPos == 0) {

                // create
                lastBlocks = new DoubleIntArrayCachedObject(lastBlockListSize);

                initialiseTableSpace(directorySpaceManager);
                lastBlockStore.add(lastBlocks, true);

                blockPos = getFileBlockPosFromPosition(lastBlocks.getPos());

                rootBlock.setValue(dirBlockSize - 1, blockPos);
            } else {
                position = getPositionFromFileBlock(blockPos);
                lastBlocks =
                    (DoubleIntArrayCachedObject) lastBlockStore.get(position,
                        true);

                initialiseTableSpace(directorySpaceManager);
            }
        }
    }

    BlockObjectStore getRootStore() {

        return new BlockObjectStore(cache, directorySpaceManager,
                                    IntArrayCachedObject.class,
                                    IntArrayCachedObject.fileSizeFactor
                                    * dirBlockSize, dirBlockSize);
    }

    BlockObjectStore getDirectoryStore() {

        return new BlockObjectStore(cache, directorySpaceManager,
                                    DirectoryBlockCachedObject.class,
                                    DirectoryBlockCachedObject.fileSizeFactor
                                    * dirBlockSize, dirBlockSize);
    }

    BlockObjectStore getBitMapStore() {

        return new BlockObjectStore(cache, directorySpaceManager,
                                    BitMapCachedObject.class,
                                    bitmapStorageSize, bitmapIntSize);
    }

    BlockObjectStore getLastBlockStore() {

        return new BlockObjectStore(cache, directorySpaceManager,
                                    DoubleIntArrayCachedObject.class,
                                    DoubleIntArrayCachedObject.fileSizeFactor
                                    * lastBlockListSize, lastBlockListSize);
    }

    private void initialiseNewSpaceDirectory() {

        long filePosition       = DataFileCache.Positions.MAX_INITIAL_FREE_POS;
        int  dirSpaceBlockCount = 1;

        cache.enlargeFileSpace(fileBlockSize);
        directorySpaceManager.initialiseFileBlock(null, filePosition,
                fileBlockSize);

        rootBlock = new IntArrayCachedObject(dirBlockSize);

        rootStore.add(rootBlock, true);

        lastBlocks = new DoubleIntArrayCachedObject(lastBlockListSize);

        lastBlockStore.add(lastBlocks, true);

        int blockPos = getFileBlockPosFromPosition(lastBlocks.getPos());

        rootBlock.setValue(dirBlockSize - 1, blockPos);
        createFileBlocksInDirectory(0, dirSpaceBlockCount, tableIdDirectory);
    }

    /**
     * The space for a new directory block must be added to the directorySpaceManager
     * before createFileBlocksInDirectory is called, otherwise there is no space
     * to create the bit-map
     */
    private void ensureDirectorySpaceAvailable(int blockCount) {

        long dirObjectSize = (long) bitmapStorageSize * blockCount;

        dirObjectSize += DirectoryBlockCachedObject.fileSizeFactor
                         * dirBlockSize;

        boolean hasRoom = directorySpaceManager.hasFileRoom(dirObjectSize);

        if (!hasRoom) {
            int  index              = getBlockIndexLimit();
            long filePosition       = (long) index * fileBlockSize;
            long dirSpaceBlockCount = dirObjectSize / fileBlockSize + 1;
            long delta              = dirSpaceBlockCount * fileBlockSize;

            cache.enlargeFileSpace(filePosition + delta);
            directorySpaceManager.addFileBlock(filePosition,
                                               filePosition + delta);
            createFileBlocksInDirectory(index, (int) dirSpaceBlockCount,
                                        tableIdDirectory);
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

        int  index        = getBlockIndexLimit();
        long filePosition = (long) index * fileBlockSize;
        long delta        = (long) blockCount * fileBlockSize;

        cache.enlargeFileSpace(filePosition + delta);
        createFileBlocksInDirectory(index, blockCount, tableId);

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
        int bitmapBlockPos = getFileBlockPosFromPosition(bitMap.getPos());
        int blockOffset    = fileBlockIndex % dirBlockSize;
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

        int  blockPos = rootBlock.getValue(indexInRoot);
        long position = getPositionFromFileBlock(blockPos);

        if (position == 0) {
            return null;
        }

        return (DirectoryBlockCachedObject) directoryStore.get(position, keep);
    }

    private void createDirectory(int fileBlockIndex) {

        DirectoryBlockCachedObject directory;

        directory = new DirectoryBlockCachedObject(dirBlockSize);

        directoryStore.add(directory, false);

        int indexInRoot   = fileBlockIndex / dirBlockSize;
        int blockPosition = getFileBlockPosFromPosition(directory.getPos());

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

        if (spaceId == tableIdDefault) {
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
                manager = new TableSpaceManagerBlocks(
                    this, spaceId, fileBlockSize,
                    cache.database.logger.propMaxFreeBlocks, dataFileScale);

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

            lastBlocks.removeKey(spaceId);

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

    public void freeTableSpace(int spaceId, LongLookup spaceList, long offset,
                               long limit) {

        if (spaceList.size() == 0 && offset == limit) {
            return;
        }

        cache.writeLock.lock();

        try {
            ba.initialise(true);

            try {
                long position;
                int  units;

                for (int i = 0; i < spaceList.size(); i++) {
                    position = spaceList.getLongKey(i);
                    units    = (int) spaceList.getLongValue(i);

                    freeTableSpacePart(position, units);
                }

                position = offset / dataFileScale;
                units    = (int) ((limit - offset) / dataFileScale);

                freeTableSpacePart(position, units);
            } finally {
                ba.reset();
            }
        } finally {
            cache.writeLock.unlock();
        }
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
                        ba.setTable(tableIdSetAside);
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

                    fragment += (long) ba.getFreeSpaceValue() * dataFileScale;

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

                if (tableSpace.getSpaceID() == tableIdDirectory
                        || tableSpace.getFileBlockIndex() != -1) {
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

                int lastBlockIndex = tableSpace.getFileBlockIndex();
                int spaceId;

                if (lastBlockIndex >= 0) {
                    spaceId = tableSpace.getSpaceID();

                    lastBlocks.addKey(spaceId, lastBlockIndex);
                }
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

        int                          count = rootBlock.getNonZeroSize();
        DirectoryBlockCachedObject[] directoryList;

        directoryList = new DirectoryBlockCachedObject[count];

        for (int i = 0; i < directoryList.length; i++) {
            directoryList[i] = getDirectoryByIndex(i, false);
        }

        return directoryList;
    }

    /**
     * return keys are file block indexes, values are bad (off) space ids
     * or tableIdDirectory when two bitmpas have the same pointer
     */
    DoubleIntIndex checkDirectorySpaces() {

        DirectoryBlockCachedObject[] directoryList   = getDirectoryList();
        DoubleIntIndex               offspaceBitmaps = new DoubleIntIndex(8);
        DoubleIntIndex               positionBitmaps = new DoubleIntIndex(8);

        for (int i = 0; i < directoryList.length; i++) {
            DirectoryBlockCachedObject dir        = directoryList[i];
            long                       position   = dir.getPos();
            int                        spaceId    = findTableSpace(position);
            int                        blockIndex = i;
            int blockPos = rootBlock.getValue(blockIndex);
            boolean                    result;
            int count = dir.getStorageSize() / fixedDiskBlockSize;

            for (int j = 0; j < count; j++) {
                result = positionBitmaps.addUnique(blockPos, blockIndex);
            }

            int[] bitMapAddress = dir.getBitmapAddressArray();

            for (int j = 0; j < bitMapAddress.length; j++) {
                blockPos = dir.getBitmapAddress(j);

                if (blockPos == 0) {
                    break;
                }

                position   = getPositionFromFileBlock(blockPos);
                spaceId    = findTableSpace(position);
                blockIndex = i * dirBlockSize + j;

                if (spaceId != tableIdDirectory) {
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

        DirectoryBlockCachedObject[] directoryList   = getDirectoryList();
        DoubleIntIndex               offspaceBitmaps = new DoubleIntIndex(8);
        int                          mismatchCount   = 0;

        for (int i = 0; i < directoryList.length; i++) {
            DirectoryBlockCachedObject dir = directoryList[i];
            int[] bitMapAddress            = dir.getBitmapAddressArray();

            for (int j = 0; j < bitMapAddress.length; j++) {
                int blockPos = dir.getBitmapAddress(j);

                if (blockPos == 0) {
                    break;
                }

                long position   = getPositionFromFileBlock(blockPos);
                int  spaceId    = findTableSpace(position);
                int  blockIndex = i * dirBlockSize + j;
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

    private int findLastFreeSpace(int spaceId) {
        return lastBlocks.getValue(spaceId, -1);
    }

    public void initialiseTableSpace(TableSpaceManagerBlocks tableSpace) {

        int spaceId        = tableSpace.getSpaceID();
        int lastBlockIndex = tableSpace.getFileBlockIndex();

        if (lastBlockIndex < 0) {
            lastBlockIndex = findLastFreeSpace(spaceId);
        }

        if (lastBlockIndex >= 0) {
            long position = (long) lastBlockIndex * fileBlockItemCount;
            int  id       = findTableSpace(position);

            if (id != spaceId) {
                lastBlockIndex = -1;
            }
        }

        if (lastBlockIndex < 0) {
            lastBlockIndex = findLargestFreeSpace(spaceId);
        }

        if (lastBlockIndex < 0) {
            return;
        }

        if (hasFreeSpace(spaceId, lastBlockIndex)) {
            initialiseTableSpace(tableSpace, lastBlockIndex);
        }
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
                tableSpace.initialiseFileBlock(null, blockPos + fileBlockSize
                                               - (long) freeItems
                                                 * dataFileScale, blockPos
                                                     + fileBlockSize);
            } else {
                cache.logSevereEvent("space manager error - recovered", null);
            }
        } finally {
            ba.reset();
        }
    }

    /**
     * input is in units of fixedBlockSizeUnit
     * output is in units of dataFileScale
     */
    long getPositionFromFileBlock(int fixedBlockPos) {
        return fixedBlockPos * (long) (fixedDiskBlockSize / dataFileScale);
    }

    int getFileBlockPosFromPosition(long position) {
        return (int) (position / (fixedDiskBlockSize / dataFileScale));
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
                    position *= (fixedDiskBlockSize / dataFileScale);
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

                if (currentId != tableIdSetAside) {
                    setTable(tableIdEmpty);
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
