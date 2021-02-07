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


package org.hsqldb.trigger;

import org.hsqldb.HsqlException;

// fredt@users 20030727 - signature altered to support update triggers
// fredt@users 20190327 - moved to own package

/**
 * The interface a HyperSQL TRIGGER must implement. The user-supplied class that
 * implements this must have a default constructor.<p>
 *
 * Contents of oldRow[] and newRow[] in each type of trigger.
 *
 * <ul>
 * <li>
 * AFTER INSERT
 * oldRow[] contains single String object = "Statement-level".
 * </li>
 * <li>
 * AFTER UPDATE
 * oldRow[] contains single String object = "Statement-level".
 * </li>
 * <li>
 * AFTER DELETE
 * oldRow[] contains single String object = "Statement-level".
 * </li>
 * <li>
 * BEFORE INSERT FOR EACH ROW
 * newRow[] contains data about to be inserted and this can
 * be modified within the trigger such that modified data gets written to the
 * database.
 * </li>
 * <li>
 * AFTER INSERT FOR EACH ROW
 * newRow[] contains data just inserted into the table.
 * </li>
 * <li>
 * BEFORE UPDATE FOR EACH ROW
 * oldRow1[] contains currently stored data and not the data that is about to be
 * updated.
 * </li>
 * <li>
 * newRow[] contains the data that is about to be updated.
 * </li>
 * <li>
 * AFTER UPDATE FOR EACH ROW
 * oldRow1[] contains old stored data.
 * newRow[] contains the new data.
 * </li>
 * <li>
 * BEFORE DELETE FOR EACH ROW
 * oldRow1[] contains row data about to be deleted.
 * </li>
 * <li>
 * AFTER DELETE FOR EACH ROW
 * oldRow1[] contains row data that has been deleted.
 * </li>
 * </ul>
 * (List compiled by Andrew Knight quozzbat@users)
 *
 * @author Peter Hudson (peterhudson@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.0
 */
public interface Trigger {

    // type of trigger
    int INSERT_AFTER      = 0;
    int DELETE_AFTER      = 1;
    int UPDATE_AFTER      = 2;
    int INSERT_AFTER_ROW  = 3;
    int DELETE_AFTER_ROW  = 4;
    int UPDATE_AFTER_ROW  = 5;
    int INSERT_BEFORE_ROW = 6;
    int DELETE_BEFORE_ROW = 7;
    int UPDATE_BEFORE_ROW = 8;

    /**
     * The method invoked upon each triggered action.
     *
     * <p> type contains the integer index id for trigger type, e.g.
     * TriggerDef.INSERT_AFTER
     *
     * <p> For all triggers defined as default FOR EACH STATEMENT both
     *  oldRow and newRow are null.
     *
     * <p> For triggers defined as FOR EACH ROW, the following will apply:
     *
     * <p> When UPDATE triggers are fired, oldRow contains the existing values
     * of the table row and newRow contains the new values.
     *
     * <p> For INSERT triggers, oldRow is null and newRow contains the table row
     * to be inserted. For DELETE triggers, newRow is null and oldRow contains
     * the table row to be deleted.
     *
     * <p> For error conditions, users can construct an HsqlException using one
     * of the static methods of org.hsqldb.error.Error with a predefined
     * SQL State from org.hsqldb.error.ErrorCode.
     *
     * @param type the type as one of the int values defined in the interface
     * @param trigName the name of the trigger
     * @param tabName the name of the table upon which the triggered action is
     *   occurring
     * @param oldRow the old row
     * @param newRow the new row
     * @throws HsqlException the preferred type of exception thrown by the method
     */
    void fire(int type, String trigName, String tabName, Object[] oldRow,
              Object[] newRow);
}
