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
 * A list which is also a set of int primitives which maintains the insertion
 * order of the elements and allows access by index. Iterators return the keys
 * in the index order.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.9.0
 */
public class OrderedIntHashSet extends IntHashSet {

    public OrderedIntHashSet() {
        this(8);
    }

    public OrderedIntHashSet(int initialCapacity) throws IllegalArgumentException {

        super(initialCapacity);

        isList = true;
    }

    public OrderedIntHashSet(int[] elements) {

        super(elements.length);

        isList = true;

        addAll(elements);
    }

    public OrderedIntHashSet(int[] elementsA, int[] elementsB) {

        super(elementsA.length + elementsB.length);

        isList = true;

        addAll(elementsA);
        addAll(elementsB);
    }

    public boolean insert(int index,
                          int key) throws IndexOutOfBoundsException {

        if (index < 0 || index > size()) {
            throw new IndexOutOfBoundsException();
        }

        if (contains(key)) {
            return false;
        }

        if (index < size()) {
            super.insertRow(index);
        }

        return add(key);
    }

    public boolean remove(int key) {
        return (Boolean) super.remove(key, 0, null, null, false, true);
    }

    public void removeEntry(int index) throws IndexOutOfBoundsException {
        checkRange(index);

        int key = intKeyTable[index];

        super.remove(key, 0, null, null, false, true);
    }

    public int get(int index) {

        checkRange(index);

        return intKeyTable[index];
    }

    public int getIndex(int value) {
        return getLookup(value);
    }

    public int getOrderedStartMatchCount(int[] array) {

        int i = 0;

        for (; i < array.length; i++) {
            if (i >= size() || get(i) != array[i]) {
                break;
            }
        }

        return i;
    }

    public boolean addAll(OrderedIntHashSet set) {

        int oldSize = size();
        int setSize = set.size();

        for (int i = 0; i < setSize; i++) {
            int value = set.get(i);

            add(value);
        }

        return oldSize != size();
    }

    private void checkRange(int i) {

        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
    }
}
