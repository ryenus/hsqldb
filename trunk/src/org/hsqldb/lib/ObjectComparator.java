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


package org.hsqldb.lib;

/**
 * Interface for equality comparison.<p>
 *
 * The equals and hashCode methods of this interace are used instead of
 * the methods of the compared Objects.<p>
 *
 * The DefaultComparator implmentation calls the Object's methods.<p>
 *
 * The IdentityComparator implementation uses {@code ==} instead of the equals
 * method of the Object.<p>
 *
 * Updated for generics fredt@users.
 *
 * @author fredt@users
 * @version 2.6.0
 * @since 2.0
 */
public interface ObjectComparator<T> {

    boolean equals(T a, T b);

    int hashCode(T a);

    long longKey(T a);

    ObjectComparator defaultComparator = new DefaultComparator();

    ObjectComparator identityComparator = new IdentityComparator();

    /**
     * Comparator that uses identity for Object equality.
     */
    class IdentityComparator implements ObjectComparator {

        public boolean equals(Object a, Object b) {
            return a == b;
        }

        public int hashCode(Object a) {
            return a == null ? 0 :
                               a.hashCode();
        }

        public long longKey(Object a) {
            return 0L;
        }
    }

    /**
     * Comparator that uses the equals and hash code methods of Objects.
     */
    class DefaultComparator implements ObjectComparator {

        public boolean equals(Object a, Object b) {
            return a == b || (a != null && a.equals(b));
        }

        public int hashCode(Object a) {
            return a == null ? 0 :
                               a.hashCode();
        }

        public long longKey(Object a) {
            return 0L;
        }
    }

}
