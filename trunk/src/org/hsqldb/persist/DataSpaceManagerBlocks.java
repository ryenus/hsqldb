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

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.3.0
 */
public class DataSpaceManagerBlocks implements DataSpaceManager {

    //
    DataFileCache           cache;
    TableSpaceManagerBlocks defaultSpaceManager;
    TableSpaceManagerBlocks directorySpaceManager;

    //
    BlockObjectStore rootStore;
    BlockObjectStore directoryStore;
    BlockObjectStore bitMapStore;

    //
    IntArrayCachedObject rootBlock;

    //
    DirectoryBlockCachedObject firstDirectory;
    boolean                    isNew;

    // todo - initialise
    int spaceIdSequence = tableIdFirst;

    //
    int blockSize     = 1024 * 2;
    int bitmapIntSize = 1024 * 2;
    int fileBlockSize;

    //
    int totalFileBlockCount;

    public DataSpaceManagerBlocks(DataFileCache cache) {

        this.cache    = cache;
        fileBlockSize = bitmapIntSize * 32 * cache.dataFileScale;

        //
        directorySpaceManager = new TableSpaceManagerBlocks(this,
                tableIdDirectory, fileBlockSize,
                cache.database.logger.propMaxFreeBlocks, cache.dataFileScale,
                0);
        defaultSpaceManager = new TableSpaceManagerBlocks(this,
                tableIdDefault, fileBlockSize,
                cache.database.logger.propMaxFreeBlocks, cache.dataFileScale,
                0);

        //
        rootStore = new BlockObjectStore(cache, directorySpaceManager,
                                         IntArrayCachedObject.class,
                                         IntArrayCachedObject.fileSizeFactor
                                         * blockSize, blockSize);
        directoryStore =
            new BlockObjectStore(cache, directorySpaceManager,
                                 DirectoryBlockCachedObject.class,
                                 DirectoryBlockCachedObject.fileSizeFactor
                                 * blockSize, blockSize);
        bitMapStore = new BlockObjectStore(cache, directorySpaceManager,
                                           BitMapCachedObject.class,
                                           BitMapCachedObject.fileSizeFactor
                                           * bitmapIntSize, bitmapIntSize);

        //
        if (cache.spaceManagerPosition == 0) {
            initNewSpaceDirectory();

            cache.spaceManagerPosition = rootBlock.getPos()
                                         * cache.dataFileScale;

            cache.setFileModified();

            isNew = true;
        } else {
            long pos = cache.spaceManagerPosition / cache.dataFileScale;

            rootBlock = (IntArrayCachedObject) rootStore.get(pos, true);

            // integrity check
            if (getBlockIndexLimit() < 2) {
                throw Error.error(ErrorCode.FILE_IO_ERROR);
            }

            spaceIdSequence = getMaxSpaceId() + 1;
        }

        firstDirectory = getDirectory(0, true);
    }

    void initNewSpaceDirectory() {

        long currentSize = cache.fileFreePosition;
        long totalBlocks = currentSize / fileBlockSize + 1;
        long lastFreePosition = cache.enlargeFileSpace(totalBlocks
            * fileBlockSize - currentSize);

        defaultSpaceManager.initialiseFileBlock((totalBlocks - 1)
                * fileBlockSize, lastFreePosition, cache.fileFreePosition);

        long directoryBlocksSize    = calculateDirectorySpace(totalBlocks);
        int  defaultSpaceBlockCount = (int) totalBlocks;
        int directorySpaceBlockCount = (int) (directoryBlocksSize
                                              / fileBlockSize);

        lastFreePosition = cache.enlargeFileSpace(directoryBlocksSize);

        // file block is empty
        directorySpaceManager.initialiseFileBlock(lastFreePosition,
                lastFreePosition, cache.fileFreePosition);

        IntArrayCachedObject root = new IntArrayCachedObject(blockSize);

        rootStore.add(null, root, false);

        rootBlock = (IntArrayCachedObject) rootStore.get(root.getPos(), true);

        createFileSpaceInDirectory(defaultSpaceBlockCount,
                                   directorySpaceBlockCount, tableIdDirectory);
        createFileSpaceInDirectory(0, defaultSpaceBlockCount, tableIdDefault);

        int index = getBlockIndexLimit();

        // integrity check
        if ((long) index * fileBlockSize != cache.fileFreePosition) {
            throw Error.error(ErrorCode.FILE_IO_ERROR);
        }
    }

    private long calculateDirectorySpace(long blockCount) {

        long currentSize = IntArrayCachedObject.fileSizeFactor * blockSize;    // root

        currentSize += (long) DirectoryBlockCachedObject.fileSizeFactor
                       * (blockCount + blockSize);      // directory - approx
        currentSize += (long) BitMapCachedObject.fileSizeFactor
                       * bitmapIntSize * blockCount;    // bitmaps
        currentSize = (currentSize / fileBlockSize + 1) * fileBlockSize;

        return currentSize;
    }

    /**
     * try available blocks first, then get fresh block
     */
    public long getFileSpace(int tableId, int blockCount) {

        cache.writeLock.lock();

        try {
            int index = getExistingBlockIndex(tableId, blockCount);

            if (index > 0) {
                return index * this.fileBlockSize;
            } else {
                return getNewFileSpace(tableId, blockCount);
            }
        } finally {
            cache.writeLock.unlock();
        }
    }

    public long getNewFileSpace(int tableId, int blockCount) {

        cache.writeLock.lock();

        try {
            if (!directorySpaceManager.hasFileRoom(
                    BitMapCachedObject.fileSizeFactor * bitmapIntSize
                    * blockCount)) {
                long lastFreePosition = cache.enlargeFileSpace(fileBlockSize);

                directorySpaceManager.addFileBlock(lastFreePosition,
                                                   lastFreePosition,
                                                   lastFreePosition
                                                   + fileBlockSize);

                int index = getBlockIndexLimit();

                createFileSpaceInDirectory(index, 1, tableIdDirectory);

                index = getBlockIndexLimit();

                // integrity check
                if ((long) index * fileBlockSize != cache.fileFreePosition) {
                    throw Error.error(ErrorCode.FILE_IO_ERROR);
                }

                if (tableId == tableIdDirectory) {
                    return lastFreePosition;
                }
            }

            int index = getBlockIndexLimit();

            // integrity check
            if ((long) index * fileBlockSize != cache.fileFreePosition) {
                throw Error.error(ErrorCode.FILE_IO_ERROR);
            }

            long filePosition = cache.enlargeFileSpace(blockCount
                * fileBlockSize);

            createFileSpaceInDirectory(index, blockCount, tableId);

            // integrity check
            index = getBlockIndexLimit();

            if ((long) index * fileBlockSize != cache.fileFreePosition) {
                throw Error.error(ErrorCode.FILE_IO_ERROR);
            }

            return filePosition;
        } finally {
            cache.writeLock.unlock();
        }
    }

    void createFileSpaceInDirectory(int fileBlockIndex, int blockCount,
                                    int tableId) {

        for (int i = 0; i < blockCount; i++) {
            createFileSpaceInDirectory(fileBlockIndex + i, tableId);
        }
    }

    void createFileSpaceInDirectory(int fileBlockIndex, int tableId) {

        DirectoryBlockCachedObject directory =
            getOrCreateDirectory(fileBlockIndex);
        int index = fileBlockIndex % blockSize;

        //
        BitMapCachedObject bitMap =
            (BitMapCachedObject) bitMapStore.getNewInstance(bitmapIntSize);

        bitMapStore.add(null, bitMap, false);

        int bitmapBlockPos = (int) (bitMap.getPos() * cache.dataFileScale
                                    / fixedBlockSizeUnit);

        updateDirectory(directory, index, tableId, bitmapBlockPos);
    }

    DirectoryBlockCachedObject getDirectory(int fileBlockIndex, boolean keep) {

        DirectoryBlockCachedObject directory;
        int                        rootIndex = fileBlockIndex / blockSize;
        long position = rootBlock.getIntArray()[rootIndex];

        if (position == 0) {
            return null;
        } else {
            position *= (fixedBlockSizeUnit / cache.dataFileScale);
            directory =
                (DirectoryBlockCachedObject) directoryStore.get(position,
                    keep);
        }

        return directory;
    }

    DirectoryBlockCachedObject getOrCreateDirectory(int fileBlockIndex) {

        DirectoryBlockCachedObject directory;
        int                        rootIndex = fileBlockIndex / blockSize;
        long position = rootBlock.getIntArray()[rootIndex];

        if (position == 0) {
            directory = createDirectory(fileBlockIndex);
        } else {
            position *= (fixedBlockSizeUnit / cache.dataFileScale);
            directory =
                (DirectoryBlockCachedObject) directoryStore.get(position,
                    false);
        }

        return directory;
    }

    DirectoryBlockCachedObject createDirectory(int blockIndex) {

        DirectoryBlockCachedObject directory;

        directory = new DirectoryBlockCachedObject(blockSize);

        directoryStore.add(null, directory, false);

        rootBlock.getIntArray()[blockIndex / blockSize] =
            (int) (directory.getPos()
                   / (fixedBlockSizeUnit / cache.dataFileScale));
        rootBlock.hasChanged = true;

        return directory;
    }

    void updateDirectory(DirectoryBlockCachedObject directory, int offset,
                         int tableId, int bitmapBlockPos) {

        directory =
            (DirectoryBlockCachedObject) directoryStore.get(directory.getPos(),
                true);
        directory.getTableIdArray()[offset]       = tableId;
        directory.getBitmapAddressArray()[offset] = bitmapBlockPos;
        directory.hasChanged                      = true;

        directory.keepInMemory(false);
    }

    int getBlockIndexLimit() {

        DirectoryBlockCachedObject directory = null;

        for (int i = 0; i < blockSize * blockSize; i++) {
            int offset = i % blockSize;

            if (offset == 0) {
                long position = rootBlock.getIntArray()[i / blockSize];

                if (position == 0) {
                    return i;
                }

                directory = getDirectory(i, false);
            }

            if (directory.getBitmapAddressArray()[offset] == 0) {
                return i;
            }
        }

        return blockSize * blockSize;
    }

    int getMaxSpaceId() {

        DirectoryBlockCachedObject directory = null;
        int                        maxId     = tableIdDefault;

        for (int i = 0; i < blockSize * blockSize; i++) {
            int offset = i % blockSize;

            if (offset == 0) {
                long position = rootBlock.getIntArray()[i / blockSize];

                if (position == 0) {
                    return maxId;
                }

                directory = getDirectory(i, false);
            }

            if (directory.getBitmapAddressArray()[offset] == 0) {
                return maxId;
            }

            int currentId = directory.getTableIdArray()[offset];

            if (currentId > maxId) {
                maxId = currentId;
            }
        }

        return maxId;
    }

    int getExistingBlockIndex(int tableId, int blockCount) {

        DirectoryBlockCachedObject directory  = null;
        int                        foundIndex = Integer.MIN_VALUE;

        for (int i = 0; i < blockSize * blockSize; i++) {
            int offset = i % blockSize;

            if (offset == 0) {
                directory = getDirectory(i, false);

                if (directory == null) {
                    return -1;
                }
            }

            if (directory.getBitmapAddressArray()[offset] == 0) {
                return -1;
            }

            if (directory.getTableIdArray()[offset] == tableIdEmpty) {
                if (blockCount == 1) {
                    foundIndex = i;

                    setDirectoryBlocksAsTable(tableId, foundIndex, blockCount);

                    return i;
                }

                if (foundIndex == Integer.MIN_VALUE) {
                    foundIndex = i;
                }

                if (i - foundIndex + 1 == blockCount) {
                    setDirectoryBlocksAsTable(tableId, foundIndex, blockCount);

                    return foundIndex;
                }
            } else {
                foundIndex = Integer.MIN_VALUE;
            }
        }

        return -1;
    }

    /**
     * index and blockCount always valid
     */
    void setDirectoryBlocksAsTable(int tableId, int index, int blockCount) {

        int                        directoryIndex = -1;
        DirectoryBlockCachedObject directory      = null;

        for (int i = index; i < index + blockCount; i++) {
            if (directoryIndex != i / blockSize) {
                if (directory != null) {
                    directory.setInMemory(false);
                }

                directory      = getDirectory(i, true);
                directoryIndex = i / blockSize;
            }

            int offset = i % blockSize;

            directory.getTableIdArray()[offset] = tableId;
        }

        directory.setInMemory(false);
    }

    void release(long position, int size) {

        int fileBlockIndex = (int) (position / fileBlockSize);
        int offset         = (int) (position % fileBlockSize);
        DirectoryBlockCachedObject directory =
            (DirectoryBlockCachedObject) directoryStore.get(fileBlockIndex,
                false);
        long bitMapAddress =
            directory.getBitmapAddressArray()[fileBlockIndex % blockSize];
        BitMapCachedObject bitMap =
            (BitMapCachedObject) bitMapStore.get(bitMapAddress, true);

        bitMap.getBitMap().setRange(offset, size / cache.dataFileScale);
        bitMap.keepInMemory(false);
    }

    int getNewSpaceId() {
        return spaceIdSequence++;
    }

    public TableSpaceManager getDefaultTableSpace() {
        return defaultSpaceManager;
    }

    public void resetDataFile(DataFileCache cache) {

        // todo
    }

    public TableSpaceManager getTableSpace(int spaceId) {

        if (spaceId == DataSpaceManager.tableIdDefault) {
            return defaultSpaceManager;
        }

        if (spaceId >= spaceIdSequence) {
            spaceIdSequence = spaceId + 1;
        }

        return new TableSpaceManagerBlocks(
            this, spaceId, fileBlockSize,
            cache.database.logger.propMaxFreeBlocks, cache.dataFileScale, 0);
    }

    public TableSpaceManager getNewTableSpace() {

        int spaceId = getNewSpaceId();

        return new TableSpaceManagerBlocks(
            this, spaceId, fileBlockSize,
            cache.database.logger.propMaxFreeBlocks, cache.dataFileScale, 0);
    }

    public void freeTableSpace(int spaceId) {

        if (spaceId == tableIdDefault) {
            return;
        }

        cache.writeLock.lock();

        try {
            DirectoryBlockCachedObject directory = null;

            for (int i = 0; i < blockSize * blockSize; i++) {
                int offset = i % blockSize;

                if (offset == 0) {
                    long position = rootBlock.getIntArray()[i / blockSize];

                    if (position == 0) {
                        return;
                    }

                    if (directory != null) {
                        directory.setInMemory(false);
                    }

                    directory = getDirectory(i, true);
                }

                if (directory.getBitmapAddressArray()[offset] == 0) {
                    return;
                }

                if (directory.getTableIdArray()[offset] == spaceId) {
                    directory.getTableIdArray()[offset] = tableIdEmpty;

                    // todo - clear the bitmap
                }
            }

            directory.setInMemory(false);
        } finally {
            cache.writeLock.unlock();
        }
    }

    public long getLostBlocksSize() {
        return 0;
    }

    public long freeBlockCount() {
        return 0;
    }

    public long freeBlockSize() {
        return 0;
    }

    public boolean isModified() {
        return true;
    }
}
