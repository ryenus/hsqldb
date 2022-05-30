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

import org.hsqldb.map.BaseHashMap;

/**
 * This class does not store null keys.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.0
 * @since 1.7.2
 */
public class HashSet<E> extends BaseHashMap implements Set<E> {

    public HashSet() {
        this(8);
    }

    public HashSet(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.objectKeyOrValue,
              BaseHashMap.noKeyOrValue, false);
    }

    public HashSet(int initialCapacity,
                   ObjectComparator comparator)
                   throws IllegalArgumentException {

        this(initialCapacity);

        this.comparator = comparator;
    }

    public HashSet(Object[] valueList) {
        this(valueList.length);

        for (int i = 0; i < valueList.length; i++) {
            add((E) valueList[i]);
        }
    }

    public boolean contains(Object key) {
        return super.containsKey(key);
    }

    public boolean containsAll(Collection<?> col) {

        Iterator<?> it = col.iterator();

        while (it.hasNext()) {
            if (contains(it.next())) {
                continue;
            }

            return false;
        }

        return true;
    }

    public E getOrAdd(E key) {

        if (key == null) {
            throw new NullPointerException();
        }

        E value = get(key);

        if (value == null) {
            value = key;

            add(key);
        }

        return value;
    }

    public E get(E key) {

        if (key == null) {
            throw new NullPointerException();
        }

        int lookup = getLookup(key);

        if (lookup < 0) {
            return null;
        } else {
            return (E) objectKeyTable[lookup];
        }
    }

    /**
     * returns true if element is added
     *
     * @param key the element
     * @return true if added
     */
    public boolean add(E key) {
        if (key == null) {
            throw new NullPointerException();
        }

        return (Boolean) super.addOrUpdate(0, 0, key, null);
    }

    /** returns true if any  element is added
     *
     * @param c the Collection to add
     * @return true if any element is added
     */
    public boolean addAll(Collection<? extends E> c) {

        boolean               changed = false;
        Iterator<? extends E> it      = c.iterator();

        while (it.hasNext()) {
            changed |= add(it.next());
        }

        return changed;
    }

    /** returns true if any element is added
    *
    * @param keys the array of elements to add
    * @return true if any element is added
    */
    public boolean addAll(E[] keys) {

        boolean changed = false;

        for (int i = 0; i < keys.length; i++) {
            changed |= add(keys[i]);
        }

        return changed;
    }

    /**
     * returns true if any added
     *
     * @param keys array of keys to add
     * @param start first index to add
     * @param limit limit of index to add
     * @return true if any element was added
     */
    public boolean addAll(E[] keys, int start, int limit) {

        boolean changed = false;

        for (int i = start; i < keys.length && i < limit; i++) {
            changed |= add(keys[i]);
        }

        return changed;
    }

    /**
     * returns true if removed
     *
     * @param key Object to remove
     * @return true if removed
     */
    public boolean remove(Object key) {

        if (key == null) {
            throw new NullPointerException();
        }

        return (Boolean) super.remove(0, 0, key, null, false, false);
    }

    /**
     * returns true if all were removed
     *
     * @param c Collection of elements to remove
     * @return true if all removed
     */
    public boolean removeAll(Collection<?> c) {

        Iterator<?> it = c.iterator();
        boolean  result = true;

        while (it.hasNext()) {
            result &= remove(it.next());
        }

        return result;
    }

    public boolean retainAll(Collection<?> c) {

        boolean changed = false;

        Iterator<?> it = iterator();

        while (it.hasNext()) {
            if (!c.contains(it.next())) {
                it.remove();
                changed = true;
            }
        }

        return changed;
    }

    /**
     * returns true if all were removed
     *
     * @param keys E[]
     * @return boolean
     */
    public boolean removeAll(E[] keys) {

        boolean result = true;

        for (int i = 0; i < keys.length; i++) {
            result &= remove(keys[i]);
        }

        return result;
    }

    public int getCommonElementCount(Set<E> other) {

        int count = 0;

        Iterator<E> it = iterator();

        while (it.hasNext()) {
            if (other.contains(it.next())) {
                count++;
            }
        }

        return count;
    }

    public <T> T[] toArray(T[] a) {
        return toArray(a, true);
    }

    public Object[] toArray() {
        return toArray(true);
    }

    public Iterator<E> iterator() {
        return new BaseHashIterator(true);
    }

    /**
     * Returns a String like "[Drei, zwei, Eins]", exactly like
     * java.util.HashSet.
     *
     * @return String representation
     */
    public String toString() {

        Iterator      it = iterator();
        StringBuilder sb = new StringBuilder();

        while (it.hasNext()) {
            if (sb.length() > 0) {
                sb.append(", ");
            } else {
                sb.append('[');
            }

            sb.append(it.next());
        }

        return sb.toString() + ']';
    }
}
