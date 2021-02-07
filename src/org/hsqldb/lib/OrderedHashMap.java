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
 * A Map which maintains the insertion order of the key/value pairs and allows
 * access by index. Iterators return the keys or values in the index order.<p>
 *
 * This class does not store null keys.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.60
 * @since 1.7.2
 */
public class OrderedHashMap<K, V> extends HashMap<K, V> {

    public OrderedHashMap() {
        this(8);
    }

    public OrderedHashMap(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity);

        this.isList = true;
    }

    /**
     * Returns the key stored in the entry at index position.
     *
     * @param index the index of the entry
     * @throws IndexOutOfBoundsException for invalid argument
     * @return the value stored in the entry
     */
    public K getKeyAt(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        return (K) objectKeyTable[index];
    }

    /**
     * Returns the value stored in the entry at index position.
     *
     * @param index the index of the entry
     * @throws IndexOutOfBoundsException for invalid argument
     * @return the value stored in the entry
     */
    public V getValueAt(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        return (V) objectValueTable[index];
    }

    /**
     * Same as getValueAt(index).
     *
     * @param index the index of the entry
     * @throws IndexOutOfBoundsException for invalid argument
     * @return the value stored in the entry
     */
    public V get(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        return (V) objectValueTable[index];
    }

    public V remove(Object key) {

        if (key == null) {
            throw new NullPointerException();
        }

        return (V) super.remove(0, 0, key, null, false, true);
    }

    public void removeEntry(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        Object key = objectKeyTable[index];

        super.remove(0, 0, key, null, false, true);
    }

    public boolean add(K key, V value) {

        if (key == null) {
            throw new NullPointerException();
        }

        int lookup = getLookup(key);

        if (lookup >= 0) {
            return false;
        }

        super.put(key, value);

        return true;
    }

    public V setValueAt(int index, V value) throws IndexOutOfBoundsException {

        checkRange(index);

        V returnValue = (V) objectValueTable[index];

        objectValueTable[index] = value;

        return returnValue;
    }

    public boolean insert(int index, K key, V value) throws IndexOutOfBoundsException {

        if (key == null) {
            throw new NullPointerException();
        }

        if (index < 0 || index > size()) {
            throw new IndexOutOfBoundsException();
        }


        int lookup = getLookup(key);

        if (lookup >= 0) {
            return false;
        }

        if (index < size()) {
            super.insertRow(index);
        }

        super.put(key, value);

        return true;
    }

    public boolean set(int index, K key, V value) throws IndexOutOfBoundsException {

        if (key == null) {
            throw new NullPointerException();
        }

        checkRange(index);

        if (keySet().contains(key) && getIndex(key) != index) {
            return false;
        }

        super.remove(0, 0, objectKeyTable[index], null, false, false);
        super.put(key, value);

        return true;
    }

    public boolean setKeyAt(int index, K key) throws IndexOutOfBoundsException {

        if (key == null) {
            throw new NullPointerException();
        }

        checkRange(index);

        V value = (V) objectValueTable[index];

        return set(index, key, value);
    }

    public int getIndex(K key) {
        return getLookup(key);
    }

    private void checkRange(int i) {

        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
    }
}
