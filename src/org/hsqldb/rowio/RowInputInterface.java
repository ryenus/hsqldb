package org.hsqldb.rowio;

import java.io.IOException;

import org.hsqldb.types.Type;

/**
 * Public interface for reading the data for a database row.
 *
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.7.0
 */
public interface RowInputInterface {

    long getPos();

    int getSize();

    int readType() throws IOException;

    String readString() throws IOException;

    byte readByte() throws IOException;

    short readShort() throws IOException;

    int readInt() throws IOException;

    long readLong() throws IOException;

    Object[] readData(Type[] colTypes) throws IOException;

    void resetRow(long filePos, int size) throws IOException;

    byte[] getBuffer();
}
