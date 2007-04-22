/* Copyright (c) 2001-2007, The HSQL Development Group
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


package org.hsqldb.result;

import java.io.IOException;

import org.hsqldb.HsqlException;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.rowio.RowInputBinary;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.types.Type;

/**
 * Meta data for a result set.
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.8.0
 */
public class ResultMetaData {

    public static final int RESULT_METADATA          = 1;
    public static final int SIMPLE_RESULT_METADATA   = 2;
    public static final int PARAM_METADATA           = 3;
    public static final int GENERATED_INDEX_METADATA = 4;
    public static final int GENERATED_NAME_METADATA  = 5;

    //
    private int             type;

    // always resolved for data results
    public String[]  colLabels;
    public String[]  tableNames;
    public String[]  colNames;
    public boolean[] isLabelQuoted;
    public Type[]    colTypes;

    // extra attrs, sometimes resolved
    public String[]              catalogNames;
    public String[]              schemaNames;
    public byte[]                colNullable;
    public boolean[]             isIdentity;
    public boolean[]             isWritable;
    public byte[]                paramModes;
    public String[]              classNames;
    private int                  columnCount;
    public static ResultMetaData emptyMetaData = new ResultMetaData();

    // column indexes for generated columns
    public int[] colIndexes;

    private ResultMetaData() {}

    public static ResultMetaData newSimpleResultMetaData(Type[] types) {

        ResultMetaData md = new ResultMetaData();

        md.colTypes    = types;
        md.columnCount = types.length;
        md.type        = SIMPLE_RESULT_METADATA;

        return md;
    }

    public static ResultMetaData newResultMetaData(int columns) {

        ResultMetaData md = new ResultMetaData();

        md.prepareData(columns);

        md.type = RESULT_METADATA;

        return md;
    }

    public static ResultMetaData newParameterMetaData(int columns) {

        ResultMetaData md = new ResultMetaData();

        md.prepareData(columns);

        md.type       = PARAM_METADATA;
        md.paramModes = new byte[columns];

        return md;
    }

    public static ResultMetaData newGeneratedColumnsMetaData(
            int[] columnIndexes, String[] columnNames) {

        if (columnIndexes != null) {
            ResultMetaData md = new ResultMetaData();

            md.columnCount = columnIndexes.length;
            md.type        = GENERATED_INDEX_METADATA;
            md.colIndexes  = new int[columnIndexes.length];

            for (int i = 0; i < columnIndexes.length; i++) {
                md.colIndexes[i] = columnIndexes[i] - 1;
            }

            return md;
        } else if (columnNames != null) {
            ResultMetaData md = new ResultMetaData();

            md.columnCount = columnNames.length;
            md.type        = GENERATED_NAME_METADATA;
            md.colNames    = (String[]) ArrayUtil.duplicateArray(columnNames);

            return md;
        } else {
            return null;
        }
    }

    void prepareData(int columns) {

        colLabels     = new String[columns];
        tableNames    = new String[columns];
        colNames      = new String[columns];
        isLabelQuoted = new boolean[columns];
        colTypes      = new Type[columns];
        catalogNames  = new String[columns];
        schemaNames   = new String[columns];
        colNullable   = new byte[columns];
        isIdentity    = new boolean[columns];
        isWritable    = new boolean[columns];
        classNames    = new String[columns];
        columnCount   = columns;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(int count) {
        columnCount = count;
    }

    public Type[] getParameterTypes() {
        return colTypes;
    }

    public String[] getGeneratedColumnNames() {
        return colNames;
    }

    public int[] getGeneratedColumnIndexes() {
        return colIndexes;
    }

    public boolean isTableColumn(int i) {
        return tableNames[i] != null && tableNames[i].length() > 0
               && colNames[i] != null && colNames[i].length() > 0;
    }

    private void decodeTableColumnAttrs(int in, int i) {

        colNullable[i] = (byte) (in & 0x0000000f);
        isIdentity[i]  = (in & 0x00000010) != 0;
        isWritable[i]  = (in & 0x00000020) != 0;
    }

    private void writeTableColumnAttrs(RowOutputInterface out,
                                       int i)
                                       throws IOException, HsqlException {

        out.writeByte(encodeTableColumnAttrs(i));
        out.writeString(catalogNames[i] == null ? ""
                                                : catalogNames[i]);
        out.writeString(schemaNames[i] == null ? ""
                                               : schemaNames[i]);
    }

    private int encodeTableColumnAttrs(int i) {

        int out = colNullable[i];    // always between 0x00 and 0x02

        if (isIdentity[i]) {
            out |= 0x00000010;
        }

        if (isWritable[i]) {
            out |= 0x00000020;
        }

        return out;
    }

    private void readTableColumnAttrs(RowInputBinary in,
                                      int i)
                                      throws IOException, HsqlException {

        decodeTableColumnAttrs(in.readByte(), i);

        catalogNames[i] = in.readString();
        schemaNames[i]  = in.readString();
    }

    ResultMetaData(RowInputBinary in) throws HsqlException, IOException {

        type = in.readInt();

        int colCount = in.readInt();

        switch (type) {

            case SIMPLE_RESULT_METADATA : {
                colTypes = new Type[colCount];

                for (int i = 0; i < colCount; i++) {
                    int type = in.readType();

                    colTypes[i] = Type.getDefaultType(type);
                }

                return;
            }
            case GENERATED_INDEX_METADATA : {
                colIndexes = new int[colCount];

                for (int i = 0; i < colCount; i++) {
                    colIndexes[i] = in.readInt();
                }

                return;
            }
            case GENERATED_NAME_METADATA : {
                colNames = new String[colCount];

                for (int i = 0; i < colCount; i++) {
                    colNames[i] = in.readString();
                }

                return;
            }
            case PARAM_METADATA : {
                paramModes = new byte[colCount];
            }
            case RESULT_METADATA : {
                prepareData(colCount);

                for (int i = 0; i < colCount; i++) {
                    int type  = in.readType();
                    long size  = in.readLong();
                    int scale = in.readInt();

                    colTypes[i]   = Type.getType(type, 0, size, scale);
                    colLabels[i]  = in.readString();
                    tableNames[i] = in.readString();
                    colNames[i]   = in.readString();
                    classNames[i] = in.readString();

                    if (isTableColumn(i)) {
                        readTableColumnAttrs(in, i);
                    }

                    if (this.type == PARAM_METADATA) {
                        paramModes[i] = in.readByte();
                    }
                }
            }
        }
    }

    void write(RowOutputInterface out) throws HsqlException, IOException {
        write(out, columnCount);
    }

    void write(RowOutputInterface out,
               int colCount) throws HsqlException, IOException {

        out.writeInt(type);
        out.writeInt(colCount);

        switch (type) {

            case SIMPLE_RESULT_METADATA : {
                for (int i = 0; i < colCount; i++) {
                    out.writeType(colTypes[i].type);
                }

                return;
            }
            case GENERATED_INDEX_METADATA : {
                for (int i = 0; i < colCount; i++) {
                    out.writeInt(colIndexes[i]);
                }

                return;
            }
            case GENERATED_NAME_METADATA : {
                for (int i = 0; i < colCount; i++) {
                    out.writeString(colNames[i]);
                }

                return;
            }
            case PARAM_METADATA :
            case RESULT_METADATA : {
                for (int i = 0; i < colCount; i++) {
                    out.writeType(colTypes[i].type);

                    // fredt - 1.8.0 added
                    out.writeLong(colTypes[i].size());
                    out.writeInt(colTypes[i].scale());
                    out.writeString(colLabels[i] == null ? ""
                                                         : colLabels[i]);
                    out.writeString(tableNames[i] == null ? ""
                                                          : tableNames[i]);
                    out.writeString(colNames[i] == null ? ""
                                                        : colNames[i]);
                    out.writeString(classNames[i] == null ? ""
                                                          : classNames[i]);

                    if (isTableColumn(i)) {
                        writeTableColumnAttrs(out, i);
                    }

                    if (type == PARAM_METADATA) {
                        out.writeByte(paramModes[i]);
                    }
                }
            }
        }
    }
}
