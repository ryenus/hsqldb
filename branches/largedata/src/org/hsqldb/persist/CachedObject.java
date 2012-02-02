package org.hsqldb.persist;

import org.hsqldb.lib.LongLookup;
import org.hsqldb.rowio.RowOutputInterface;

/**
 * Interface for an object stored in the memory cache.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.8.0
 */
public interface CachedObject {

    CachedObject[] emptyArray = new CachedObject[]{};

    boolean isMemory();

    void updateAccessCount(int count);

    int getAccessCount();

    void setStorageSize(int size);

    int getStorageSize();

    long getPos();

    void setPos(long pos);

    boolean hasChanged();

    boolean isKeepInMemory();

    boolean keepInMemory(boolean keep);

    boolean isInMemory();

    void setInMemory(boolean in);

    void restore();

    void destroy();

    int getRealSize(RowOutputInterface out);

    void write(RowOutputInterface out);

    void write(RowOutputInterface out, LongLookup lookup);
}
