/* Copyright (c) 2001-2009, The HSQL Development Group
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

import org.hsqldb.types.IntervalType;
import org.hsqldb.types.Type;

public class ScannerTest {
    public static void main(String[] args) {

        IntervalType t       = Type.SQL_INTERVAL_DAY;
        Object       i       = null;
        String       s       = null;
        Scanner      scanner = new Scanner();

        try {
            s = "200 10";
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_HOUR,
                                             4, 0);
            i = scanner.convertToDatetimeInterval(s, t);

            s = "200 10:12:12.456789";
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 6);
            i = scanner.convertToDatetimeInterval(s, t);

            s = "200 10:12:12.456789";
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 7);
            i = scanner.convertToDatetimeInterval(s, t);

            s = "INTERVAL '200 10:12:12.' DAY(4) TO SECOND(8)";
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 5);
            i = scanner.convertToDatetimeInterval(s, t);
            s = "INTERVAL '200 10:12:12.' DAY TO SECOND";
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 5);
            i = scanner.convertToDatetimeInterval(s, t);

            /* TODO:  Fix these tests.
             * The inheritance design of the types has changed, breaking the
             * usage here.
            s = "TIME '10:12:12.'";
            t = Type.SQL_TIME;
            i = scanner.convertToDatetimeInterval(s, t);

            s = "2007-01-02 10:12:12";
            t = Type.SQL_TIMESTAMP;
            i = scanner.convertToDatetimeInterval(s, t);

            s = "200 10:00:12";
            t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                                             3, 5);
            i = scanner.convertToDatetimeInterval(s, t);
            */

        } catch (HsqlException e) {
            System.out.println(s);
        }
    }
}
