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


package org.hsqldb;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.TimestampData;

/**
 * The methods in this class perform alterations to the structure of an
 * existing table which may result in a new Table object.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.0
 * @since 2.5.0
 */
class TablePeriodWorks {

    private Table    table;
    private Session  session;

    public TablePeriodWorks(Session session, Table table) {

        this.table    = table;
        this.session  = session;
    }

    void addSystemPeriod(PeriodDefinition period) {

        if (table.systemPeriod != null) {
            throw Error.error(ErrorCode.X_42517);
        }

        TableWorks tw = new TableWorks(session, table);

        tw.addSystemPeriod(period);
    }

    void addApplicationPeriod(PeriodDefinition period) {
        throw Error.error(ErrorCode.X_0A501);
    }

    void addSystemVersioning() {

        if (table.isSystemVersioned) {
            throw Error.error(ErrorCode.X_42518);
        }

        if (table.systemPeriod == null) {
            throw Error.error(ErrorCode.X_42518);
        }

        table.isSystemVersioned = true;
    }

    void dropSystemPeriod(boolean cascade) {

        if (table.isSystemVersioned) {
            throw Error.error(ErrorCode.X_42518);
        }

        if (table.systemPeriod == null) {
            throw Error.error(ErrorCode.X_42517);
        }

        TableWorks tw = new TableWorks(session, table);

        tw.dropSystemPeriod(cascade);
    }

    void dropApplicationPeriod(boolean cascade) {
        throw Error.error(ErrorCode.X_0A501);
    }

    void dropSystemVersioning(boolean cascade) {

        if (!table.isSystemVersioned()) {
            throw Error.error(ErrorCode.X_42518);
        }

        TableWorks tw = new TableWorks(session, table);

        tw.dropSystemVersioning(cascade);

        long timestampLimit = DateTimeType.epochLimitTimestamp.getSeconds();

        removeOldRows(timestampLimit);

        table.isSystemVersioned = false;
    }

    long removeOldRows(long timestampLimit) {

        int         colIndex = table.systemPeriodEndColumn;
        long        count    = 0;
        RowIterator it       = table.rowIterator(session);

        while (it.next()) {
            TimestampData value = (TimestampData) it.getField(colIndex);

            if (value.getSeconds() < timestampLimit) {
                it.removeCurrent();
            }
        }

        return count;
    }
}
