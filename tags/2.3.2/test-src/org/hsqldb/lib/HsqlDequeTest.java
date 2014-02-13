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
package org.hsqldb.lib;

import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;
import junit.framework.Test;
import junit.framework.TestSuite;

@ForSubject(HsqlDeque.class)
public class HsqlDequeTest extends  BaseTestCase {

    public HsqlDequeTest(String name) {
        super(name);
    }

    @OfMethod({"add(java.langObject)","removeFirst()", "addFirst()", "removeFirst(), iterator()"})
    public void testHsqlDeque() {

        HsqlDeque d = new HsqlDeque();

        for (int i = 0; i < 9; i++) {
            d.add(new Integer(i));
        }

        d.removeFirst();
        d.removeFirst();
        d.add(new Integer(9));
        d.add(new Integer(10));

        for (int i = 0; i < d.size(); i++) {
            println(d.get(i));
        }

        System.out.println();
        d.add(new Integer(11));
        d.add(new Integer(12));

        for (int i = 0; i < d.size(); i++) {
            println(d.get(i));
        }

        d.addFirst(new Integer(1));
        d.addFirst(new Integer(0));
        d.addFirst(new Integer(-1));
        d.addFirst(new Integer(-2));

        for (int i = 0; i < d.size(); i++) {
            println(d.get(i));
        }

        println();
        d.removeFirst();
        d.removeFirst();
        d.removeFirst();

        for (int i = 0; i < d.size(); i++) {
            println(d.get(i));
        }

       println();

        Iterator it = d.iterator();

        for (; it.hasNext();) {
            println(it.next());
        }
    }

    public static Test suite() {
        return new TestSuite(HsqlDequeTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
