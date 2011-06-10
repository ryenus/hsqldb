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

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import java.util.Comparator;

public class HsqlArrayHeapTest extends TestCase {

    public HsqlArrayHeapTest(String name) {
        super(name);
    }

    public void testHsqlArrayHeap() {

        Comparator oc = new Comparator() {

            @Override
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

        System.out.println("isEmpty() : " + ah.isEmpty());

        int[] ai = new int[]{
            3, 99, 7, 9, -42, 2, 1, 23, -7
        };
        int least = Integer.MIN_VALUE;

        for (int i = 0; i < ai.length; i++) {
            System.out.println("add()     : new Integer(" + ai[i] + ")");
            ah.add(new Integer(ai[i]));
            System.out.println("size()    : " + ah.size());
        }

        while (ah.size() > 0) {
            int current = ((Integer) ah.remove()).intValue();

            if (current < least) {
                throw new RuntimeException("bad heap invariant");
            }

            least = current;

            System.out.println("remove()  : " + current);
            System.out.println("size()    : " + ah.size());
        }

        System.out.println("peak() : " + ah.peek());
        System.out.println("isEmpty() : " + ah.isEmpty());
        System.out.println("remove()  : " + ah.remove());
        System.out.println("size()    : " + ah.size());
        System.out.println("isEmpty() : " + ah.isEmpty());
    }

    public static Test suite() {
        return new TestSuite(HsqlArrayHeapTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
