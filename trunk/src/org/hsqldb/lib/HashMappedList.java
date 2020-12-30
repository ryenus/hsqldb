/* Copyright (c) 2001-2020, The HSQL Development Group
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

import java.lang.reflect.Array;

/**
 * Implementation of an Map which maintains the user-defined order of the keys.
 * Key/value pairs can be accessed by index or by key. Iterators return the
 * keys or values in the index order.
 *
 * This class does not store null keys.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.2
 * @since 1.7.2
 */
public class HashMappedList<K, V> extends HashMap<K, V> {

    public HashMappedList() {
        this(8);
    }

    public HashMappedList(int initialCapacity)
    throws IllegalArgumentException {
        super(initialCapacity);

        isList = true;
    }

    public V get(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        return (V) objectValueTable[index];
    }

    public V remove(Object key) {

        int lookup = getLookup(key);

        if (lookup < 0) {
            return null;
        }

        V returnValue = (V) super.remove(key);

        removeRow(lookup);

        return returnValue;
    }

    public V remove(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        return (V) remove(objectKeyTable[index]);
    }

    public boolean add(K key, V value) {

        int lookup = getLookup(key);

        if (lookup >= 0) {
            return false;
        }

        super.put(key, value);

        return true;
    }

    public V put(K key, V value) {
        return (V) super.put(key, value);
    }

    public V set(int index,
                      V value) throws IndexOutOfBoundsException {

        checkRange(index);

        V returnValue = (V) objectValueTable[index];

        objectValueTable[index] = value;

        return returnValue;
    }

    public boolean insert(int index, K key,
                          V value) throws IndexOutOfBoundsException {

        if (index < 0 || index > size()) {
            throw new IndexOutOfBoundsException();
        }

        int lookup = getLookup(key);

        if (lookup >= 0) {
            return false;
        }

        if (index == size()) {
            return add(key, value);
        }

        HashMappedList<K, V> hm = new HashMappedList<K, V>(size());

        for (int i = index; i < size(); i++) {
            hm.add(getKey(i), get(i));
        }

        for (int i = size() - 1; i >= index; i--) {
            remove(i);
        }

        for (int i = 0; i < hm.size(); i++) {
            add(hm.getKey(i), hm.get(i));
        }

        return true;
    }

    public boolean set(int index, K key,
                       V value) throws IndexOutOfBoundsException {

        checkRange(index);

        if (keySet().contains(key) && getIndex(key) != index) {
            return false;
        }

        super.remove(objectKeyTable[index]);
        super.put(key, value);

        return true;
    }

    public boolean setKey(int index,
                          K key) throws IndexOutOfBoundsException {

        checkRange(index);

        V value = (V) objectValueTable[index];

        return set(index, key, value);
    }

    public boolean setValue(int index,
                            V value) throws IndexOutOfBoundsException {

        boolean result;
        V       existing = (V) objectValueTable[index];

        if (value == null) {
            result = existing != null;
        } else {
            result = !value.equals(existing);
        }

        objectValueTable[index] = value;

        return result;
    }

    public K getKey(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        return (K) objectKeyTable[index];
    }

    public int getIndex(K key) {
        return getLookup(key);
    }

    public V[] toValuesArray(V[] array) {

        int elementCount = size();

        if (array.length < elementCount) {
            array = (V[]) Array.newInstance(array.getClass().getComponentType(),
                                        elementCount);
        }

        System.arraycopy(objectValueTable, 0, array, 0, elementCount);

        return array;
    }

    public K[] toKeysArray(K[] array) {

        int elementCount = size();

        if (array.length < elementCount) {
            array = (K[]) Array.newInstance(array.getClass().getComponentType(),
                                        elementCount);
        }

        System.arraycopy(objectKeyTable, 0, array, 0, elementCount);

        return array;
    }

    private void checkRange(int i) {

        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
    }
}
