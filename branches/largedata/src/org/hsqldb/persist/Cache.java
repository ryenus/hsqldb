package org.hsqldb.persist;

import java.util.Comparator;

import org.hsqldb.lib.ArraySort;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.StopWatch;
import org.hsqldb.store.BaseHashMap;

/**
 * New implementation of row caching for CACHED tables.<p>
 *
 * Manages memory for the cache map and its contents based on least recently
 * used clearup.<p>
 * Also provides services for selecting rows to be saved and passing them
 * to DataFileCache.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.8.0
 */
public class Cache extends BaseHashMap {

    final DataFileCache                  dataFileCache;
    private int                          capacity;         // number of Rows
    private long                         bytesCapacity;    // number of bytes
    private final CachedObjectComparator rowComparator;

//
    private CachedObject[] rowTable;
    long                   cacheBytesLength;

    // for testing
    StopWatch saveAllTimer = new StopWatch(false);
    StopWatch sortTimer    = new StopWatch(false);
    int       saveRowCount = 0;

    Cache(DataFileCache dfc) {

        super(dfc.capacity(), BaseHashMap.objectKeyOrValue,
              BaseHashMap.noKeyOrValue, true);

        maxCapacity      = dfc.capacity();
        dataFileCache    = dfc;
        capacity         = dfc.capacity();
        bytesCapacity    = dfc.bytesCapacity();
        rowComparator    = new CachedObjectComparator();
        rowTable         = new CachedObject[capacity];
        cacheBytesLength = 0;
    }

    /**
     *  Structural initialisations take place here. This allows the Cache to
     *  be resized while the database is in operation.
     */
    void resize(int capacity, long bytesCapacity) {}

    long getTotalCachedBlockSize() {
        return cacheBytesLength;
    }

    protected int getLookup(long key) {

        int          lookup = hashIndex.getLookup((int) key);
        CachedObject tempKey;

        for (; lookup >= 0; lookup = hashIndex.getNextLookup(lookup)) {
            tempKey = (CachedObject) objectKeyTable[lookup];

            if (tempKey.getPos() == key) {
                return lookup;
            }
        }

        return lookup;
    }

    /**
     * Returns a row if in memory cache.
     */
    public synchronized CachedObject get(long pos) {

        if (accessCount > ACCESS_MAX) {
            updateAccessCounts();
            resetAccessCount();
            updateObjectAccessCounts();
        }

        int lookup = getLookup(pos);

        if (lookup == -1) {
            return null;
        }

        accessTable[lookup] = ++accessCount;

        CachedObject object = (CachedObject) objectKeyTable[lookup];

        return object;
    }

    /**
     * Adds a row to the cache.
     */
    synchronized void put(long key, CachedObject row) {

        int storageSize = row.getStorageSize();

        if (size() >= capacity
                || storageSize + cacheBytesLength > bytesCapacity) {
            cleanUp();

            if (size() >= capacity) {
                forceCleanUp();
            }
        }

        if (accessCount > ACCESS_MAX) {
            updateAccessCounts();
            resetAccessCount();
            updateObjectAccessCounts();
        }

        super.addOrRemove(0, 0, row, null, false);
        row.setInMemory(true);

        cacheBytesLength += storageSize;
    }

    /**
     * Removes an object from memory cache. Does not release the file storage.
     */
    synchronized CachedObject release(long i) {

        int          hash        = (int) i;
        int          index       = hashIndex.getHashIndex(hash);
        int          lookup      = hashIndex.getLookup(hash);
        int          lastLookup  = -1;
        CachedObject returnValue = null;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            returnValue = (CachedObject) objectKeyTable[lookup];

            if (returnValue.getPos() == i) {
                break;
            }
        }

        if (lookup >= 0) {
            objectKeyTable[lookup] = null;

            hashIndex.unlinkNode(index, lastLookup, lookup);

            accessTable[lookup] = 0;
        } else {
            return null;
        }

        cacheBytesLength -= returnValue.getStorageSize();

        returnValue.setInMemory(false);

        return returnValue;
    }

    /**
     * Replace a row in the cache.
     */
    synchronized void replace(long key, CachedObject row) {

        int lookup = super.getLookup(key);

        objectKeyTable[lookup] = row;
    }

    private void updateAccessCounts() {

        CachedObject r;
        int          count;

        for (int i = 0; i < objectKeyTable.length; i++) {
            r = (CachedObject) objectKeyTable[i];

            if (r != null) {
                count = r.getAccessCount();

                if (count > accessTable[i]) {
                    accessTable[i] = count;
                }
            }
        }
    }

    private void updateObjectAccessCounts() {

        CachedObject r;
        int          count;

        for (int i = 0; i < objectKeyTable.length; i++) {
            r = (CachedObject) objectKeyTable[i];

            if (r != null) {
                count = accessTable[i];

                r.updateAccessCount(count);
            }
        }
    }

    /**
     * Reduces the number of rows held in this Cache object. <p>
     *
     * Cleanup is done by checking the accessCount of the Rows and removing
     * the rows with the lowest access count.
     *
     * Index operations require that up to 5 recently accessed rows remain
     * in the cache.
     *
     */
    private synchronized void cleanUp() {

        updateAccessCounts();

        int                          removeCount = size() / 2;
        int accessTarget = getAccessCountCeiling(removeCount, removeCount / 8);
        BaseHashMap.BaseHashIterator it          = new BaseHashIterator(true);
        int                          savecount   = 0;

        for (; it.hasNext(); ) {
            CachedObject row                = (CachedObject) it.next();
            int          currentAccessCount = it.getAccessCount();
            boolean      oldRow = currentAccessCount <= accessTarget;

            if (oldRow) {
                synchronized (row) {
                    if (row.isKeepInMemory()) {
                        it.setAccessCount(accessTarget + 1);
                    } else {
                        if (row.hasChanged()) {
                            rowTable[savecount++] = row;
                        }

                        row.setInMemory(false);
                        it.remove();

                        cacheBytesLength -= row.getStorageSize();

                        removeCount--;
                    }
                }
            }

            if (savecount == rowTable.length) {
                saveRows(savecount);

                savecount = 0;
            }
        }

        super.setAccessCountFloor(accessTarget);
        saveRows(savecount);
    }

    synchronized void forceCleanUp() {

        BaseHashMap.BaseHashIterator it = new BaseHashIterator(true);

        for (; it.hasNext(); ) {
            CachedObject row = (CachedObject) it.next();

            synchronized (row) {
                if (!row.hasChanged() && !row.isKeepInMemory()) {
                    row.setInMemory(false);
                    it.remove();

                    cacheBytesLength -= row.getStorageSize();
                }
            }
        }
    }

    private synchronized void saveRows(int count) {

        if (count == 0) {
            return;
        }

        long startTime = saveAllTimer.elapsedTime();

        rowComparator.setType(CachedObjectComparator.COMPARE_POSITION);
        sortTimer.zero();
        sortTimer.start();
        ArraySort.sort(rowTable, 0, count, rowComparator);
        sortTimer.stop();
        saveAllTimer.start();
        dataFileCache.saveRows(rowTable, 0, count);

        saveRowCount += count;

        saveAllTimer.stop();

        //
        logSaveRowsEvent(count, startTime);
    }

    /**
     * Writes out all modified cached Rows.
     */
    synchronized void saveAll() {

        Iterator it        = new BaseHashIterator(true);
        int      savecount = 0;

        for (; it.hasNext(); ) {
            if (savecount == rowTable.length) {
                saveRows(savecount);

                savecount = 0;
            }

            CachedObject r = (CachedObject) it.next();

            if (r.hasChanged()) {
                rowTable[savecount] = r;

                savecount++;
            }
        }

        saveRows(savecount);
    }

    void logSaveRowsEvent(int saveCount, long startTime) {

        StringBuffer sb = new StringBuffer();

        sb.append("cache save rows [count,time] totals ");
        sb.append(saveRowCount);
        sb.append(',').append(saveAllTimer.elapsedTime()).append(' ');
        sb.append("operation ").append(saveCount).append(',');
        sb.append(saveAllTimer.elapsedTime() - startTime).append(' ');

//
        sb.append("txts ");
        sb.append(dataFileCache.database.txManager.getGlobalChangeTimestamp());

//
        dataFileCache.database.logger.logDetailEvent(sb.toString());
    }

    /**
     * clears out the memory cache
     */
    synchronized public void clear() {

        super.clear();

        cacheBytesLength = 0;
    }

    static final class CachedObjectComparator implements Comparator {

        static final int COMPARE_LAST_ACCESS = 0;
        static final int COMPARE_POSITION    = 1;
        static final int COMPARE_SIZE        = 2;
        private int      compareType;

        CachedObjectComparator() {}

        void setType(int type) {
            compareType = type;
        }

        public int compare(Object a, Object b) {

            long diff;

            switch (compareType) {

                case COMPARE_POSITION :
                    diff = ((CachedObject) a).getPos()
                           - ((CachedObject) b).getPos();
                    break;

                case COMPARE_SIZE :
                    diff = ((CachedObject) a).getStorageSize()
                           - ((CachedObject) b).getStorageSize();
                    break;

                default :
                    return 0;
            }

            return diff == 0 ? 0
                             : diff > 0 ? 1
                                        : -1;
        }
    }
}
