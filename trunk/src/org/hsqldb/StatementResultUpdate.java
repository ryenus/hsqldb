/* Copyright (c) 2001-2010, The HSQL Development Group
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


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.navigator.RowSetNavigatorData;
import org.hsqldb.navigator.RowSetNavigatorDataChange;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.types.Type;

/**
 * Implementation of Statement for updating result rows.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public class StatementResultUpdate extends StatementDML {

    int    actionType;
    Type[] types;
    Result result;

    StatementResultUpdate() {

        super();

        writeTableNames = new HsqlName[1];
    }

    public String describe(Session session) {
        return "";
    }

    public Result execute(Session session) {

        try {
            return getResult(session);
        } catch (Throwable e) {
            return Result.newErrorResult(e, null);
        }
    }

    Result getResult(Session session) {

        checkAccessRights(session);

        Object[]        args = session.sessionContext.dynamicArguments;
        Row             row;
        PersistentStore store = baseTable.getRowStore(session);

        switch (actionType) {

            case ResultConstants.UPDATE_CURSOR : {
                row = getRow(session, args);

                /**
                 * @todo - in 2PL mode isDeleted() always returns false.
                 * While write lock prevents delete by other transactions,
                 * same-transaction deletes are not caught
                 */
                if (row == null || row.isDeleted(session, store)) {
                    throw Error.error(ErrorCode.X_24521);
                }

                RowSetNavigatorDataChange list = new RowSetNavigatorDataChange(
                    session.database.sqlEnforceTDCD,
                    session.database.sqlEnforceTDCU);
                Object[] data =
                    (Object[]) ArrayUtil.duplicateArray(row.getData());
                boolean[] columnCheck = baseTable.getNewColumnCheckList();

                for (int i = 0; i < baseColumnMap.length; i++) {
                    if (types[i] == Type.SQL_ALL_TYPES) {
                        continue;
                    }

                    data[baseColumnMap[i]]        = args[i];
                    columnCheck[baseColumnMap[i]] = true;
                }

                int[] colMap = ArrayUtil.booleanArrayToIntIndexes(columnCheck);

                list.addRow(session, row, data, baseTable.getColumnTypes(),
                            colMap);
                update(session, baseTable, list, null);

                break;
            }
            case ResultConstants.DELETE_CURSOR : {
                row = getRow(session, args);

                if (row == null || row.isDeleted(session, store)) {
                    throw Error.error(ErrorCode.X_24521);
                }

                RowSetNavigatorDataChange navigator =
                    new RowSetNavigatorDataChange(
                        session.database.sqlEnforceTDCD,
                        session.database.sqlEnforceTDCU);

                navigator.addRow(row);
                delete(session, baseTable, navigator);

                break;
            }
            case ResultConstants.INSERT_CURSOR : {
                Object[] data = baseTable.getNewRowData(session);

                for (int i = 0; i < data.length; i++) {
                    data[baseColumnMap[i]] = args[i];
                }

                return insertSingleRow(session, store, data);
            }
        }

        return Result.updateOneResult;
    }

    Row getRow(Session session, Object[] args) {

        int             rowIdIndex = result.metaData.getColumnCount();
        Long            rowId      = (Long) args[rowIdIndex];
        PersistentStore store      = baseTable.getRowStore(session);
        Row             row        = null;

        if (rowIdIndex + 2 == result.metaData.getExtendedColumnCount()) {
            Object[] data =
                ((RowSetNavigatorData) result.getNavigator()).getData(
                    rowId.longValue());

            if (data != null) {
                row = (Row) data[rowIdIndex + 1];
            }
        } else {
            int id = (int) rowId.longValue();

            row = (Row) store.get(id, false);
        }

        this.result = null;

        return row;
    }

    void setRowActionProperties(Result result, int action, Table table,
                                Type[] types, int[] columnMap) {

        this.result             = result;
        this.actionType         = action;
        this.baseTable          = table;
        this.types              = types;
        this.baseColumnMap      = columnMap;
        this.writeTableNames[0] = table.getName();
    }

    void checkAccessRights(Session session) {

        switch (type) {

            case StatementTypes.CALL : {
                break;
            }
            case StatementTypes.INSERT : {
                session.getGrantee().checkInsert(targetTable,
                                                 insertCheckColumns);

                break;
            }
            case StatementTypes.SELECT_CURSOR :
                break;

            case StatementTypes.DELETE_WHERE : {
                session.getGrantee().checkDelete(targetTable);

                break;
            }
            case StatementTypes.UPDATE_WHERE : {
                session.getGrantee().checkUpdate(targetTable,
                                                 updateCheckColumns);

                break;
            }
            case StatementTypes.MERGE : {
                session.getGrantee().checkInsert(targetTable,
                                                 insertCheckColumns);
                session.getGrantee().checkUpdate(targetTable,
                                                 updateCheckColumns);

                break;
            }
        }
    }
}
