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

import java.util.Comparator;

import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(HsqlArrayHeap.class)
public class HsqlArrayHeapTest extends BaseTestCase {

    public HsqlArrayHeapTest(String name) {
        super(name);
    }

    @OfMethod({"add(java.lang.Object)", "size()", "remove()", "peek()", "isEmpty()"})
    public void testHsqlArrayHeap() {

        Comparator<Object> oc = new Comparator<Object>() {

            public int compare(Object a, Object b) {

                if (a == b) {
                    return 0;
                }

                // null==null and smaller than any value
                if (a == null) {
                    if (b == null) {
                        return 0;
                    }

                    return -1;
                }

                if (b == null) {
                    return 1;
                }

                return ((Integer) a).intValue() - ((Integer) b).intValue();
            }
        };
        HsqlHeap ah = new HsqlArrayHeap(6, oc);

        assertTrue("isEmpty()", ah.isEmpty());

        int[] ai = new int[]{
            3, 99, 7, 9, -42, 2, 1, 23, -7
        };
        int least = Integer.MIN_VALUE;

        for (int i = 0; i < ai.length; i++) {
            println("add()     : new Integer(" + ai[i] + ")");
            ah.add(new Integer(ai[i]));
            println("size()    : " + ah.size());
        }

        while (ah.size() > 0) {
            int current = ((Integer) ah.remove()).intValue();

            println("remove()  : " + current);
            println("size()    : " + ah.size());

            assertFalse("bad heap invariant: current < least", current < least);

            least = current;
        }

        assertNull("peak()",ah.peek());
        assertTrue("isEmpty()", ah.isEmpty());
        assertNull("remove()", ah.remove());
        assertEquals("size()", 0, ah.size());
        assertTrue("isEmpty()", ah.isEmpty());
        assertFalse("isFull()", ah.isFull());
    }

    public static Test suite() {
        return new TestSuite(HsqlArrayHeapTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
