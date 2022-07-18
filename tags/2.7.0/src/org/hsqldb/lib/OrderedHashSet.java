/* Copyright (c) 2001-2022, The HSQL Development Group
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
 * A list which is also a Set which maintains the inserted order of elements and
 * allows access by index. Iterators return the elements in the index order.<p>
 *
 * This class does not store null elements.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.0
 * @since 1.9.0
 */
public class OrderedHashSet<E> extends HashSet<E> implements List<E>, Set<E> {

    public OrderedHashSet() {
        super(8);

        this.isList = true;
    }

    public OrderedHashSet(int initialCapacity) {
        super(initialCapacity);

        this.isList = true;
    }

    public OrderedHashSet(int initialCapacity, ObjectComparator<E> comparator) {
        super(initialCapacity, comparator);

        this.isList = true;
    }

    public OrderedHashSet(Object[] valueList) {
        this(valueList.length);

        for (int i = 0; i < valueList.length; i++) {
            add((E) valueList[i]);
        }
    }

    public boolean remove(Object key) {

        if (key == null) {
            throw new NullPointerException();
        }

        return (Boolean) super.remove(0, 0, key, null, false, true);
    }

    public E remove(int index) {

        checkRange(index);

        E key = (E)objectKeyTable[index];

        super.remove(0, 0, key, null, false, true);

        return key;
    }

    public void removeEntry(int index) throws IndexOutOfBoundsException {
        remove(index);
    }

    public boolean insert(int index,
                          E key) throws IndexOutOfBoundsException {

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

    public E set(int index, E key) {
        if (key == null) {
            throw new NullPointerException();
        }

        checkRange(index);

        if (contains(key) && getIndex(key) != index) {
            throw new IllegalArgumentException();
        }

        E oldKey = (E) objectKeyTable[index];

        super.remove(0, 0, oldKey, null, false, false);

        add(key);

        return oldKey;
    }

    public void add(int index, E key) {
        boolean result = insert(index, key);

        if (!result) {
            throw new IllegalArgumentException();
        }
    }

    public E get(int index) {

        checkRange(index);

        return (E) objectKeyTable[index];
    }

    public Object[] toArray() {
        Object[] array = new Object[size()];
        return toArray(array);
    }

    public <T> T[] toArray(T[] array) {
        System.arraycopy(super.objectKeyTable, 0, array, 0, array.length);

        return array;
    }

    public int indexOf(Object key) {
        return getLookup(key);
    }

    public int getIndex(Object key) {
        return getLookup(key);
    }

    public int lastIndexOf(Object o) {
        return indexOf(o);
    }

    public int getLargestIndex(OrderedHashSet<E> other) {

        int max = -1;

        for (int i = 0, size = other.size(); i < size; i++) {
            int index = indexOf(other.get(i));

            if (index > max) {
                max = index;
            }
        }

        return max;
    }

    public int getSmallestIndex(OrderedHashSet<E> other) {

        int min = -1;

        for (int i = 0, size = other.size(); i < size; i++) {
            int index = indexOf(other.get(i));

            if (index != -1) {
                if (min == -1 || index < min) {
                    min = index;
                }
            }
        }

        return min;
    }

    public int getCommonElementCount(Set<E> other) {

        int count = 0;

        for (int i = 0, size = size(); i < size; i++) {
            if (other.contains((E) objectKeyTable[i])) {
                count++;
            }
        }

        return count;
    }

    public static <E> OrderedHashSet<E> addAll(OrderedHashSet<E> first,
                                        OrderedHashSet<E> second) {

        if (second == null) {
            return first;
        }

        if (first == null) {
            first = new OrderedHashSet<E>();
        }

        first.addAll(second);

        return first;
    }

    public static <E> OrderedHashSet<E> add(OrderedHashSet<E> first, E value) {

        if (value == null) {
            return first;
        }

        if (first == null) {
            first = new OrderedHashSet<E>();
        }

        first.add(value);

        return first;
    }

    private void checkRange(int i) {

        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
    }
}
