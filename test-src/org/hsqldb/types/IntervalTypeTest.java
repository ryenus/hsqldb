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
package org.hsqldb.types;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class IntervalTypeTest extends TestCase {
    /*
     * TODO:  Re-implement test coded below as newInterval(String).
     * There is no such method.
     */

    public void testIntervalType() {

        IntervalType t = null;
        Object i = null;
        String s = null;

        //try {
            /*
         * TODO:  Re-implement test coded below as newInterval(String).
         * There is no such method.
         */
        s = "200 10";
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_YEAR_TO_MONTH,
                4, 0);
        //i = t.newInterval(s);
        s = "200 10:12:12.456789";
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                3, 6);
        //i = t.newInterval(s);
        s = " 200 10:12:12.456789  ";
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                3, 7);
        //i = t.newInterval(s);
        s = " 200 10:12:12.";
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                3, 5);
        //i = t.newInterval(s);
        s = " 200 10:12:12. ";
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                3, 5);
        //i = t.newInterval(s);
        s = " 200 10:12:12";
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                3, 5);
        //i = t.newInterval(s);
        s = " 200 10:0:12";
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                3, 5);
        //i = t.newInterval(s);
        //} catch (HsqlException e) {
        //System.out.println(s);
        //}

        //try {
        s = "20000 10";    // first part too long
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_YEAR_TO_MONTH,
                4, 0);
        //i = t.newInterval(s);

        System.out.println(s);
        //} catch (HsqlException e) {}

        //try {
        s = "2000 90";    // other part too large
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_YEAR_TO_MONTH,
                4, 0);
        //i = t.newInterval(s);

        System.out.println(s);
        //} catch (HsqlException e) {}

        //try {
        s = "200 10:12:123.456789";    // other part to long
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                3, 5);
        //i = t.newInterval(s);

        System.out.println(s);
        //} catch (HsqlException e) {}

        //try {
        s = " 200 10:12 12.456789  ";    // bad separator
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                3, 5);
        //i = t.newInterval(s);

        System.out.println(s);
        //} catch (HsqlException e) {}

        //try {
        s = " 200 10:12:12 456789  ";    // bad separator
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                3, 5);
        //i = t.newInterval(s);

        System.out.println(s);
        //} catch (HsqlException e) {}

        //try {
        s = " 200 10:12:12 .";    // bad separator
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                3, 5);
        //i = t.newInterval(s);

        System.out.println(s);
        //} catch (HsqlException e) {}

        //try {
        s = " 20000 10:12:12. ";    // first part too long
        t = IntervalType.getIntervalType(Types.SQL_INTERVAL_DAY_TO_SECOND,
                3, 5);
        //i = t.newInterval(s);

        System.out.println(s);
        //} catch (HsqlException e) {}
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(IntervalTypeTest.class);

        return suite;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
