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
 * A Map of int primitives to Object values which maintains the insertion order
 * of the key/value pairs and allows access by index. Iterators return the keys
 * or values in the index order.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 2.0.0
 */
public class OrderedIntKeyHashMap<V> extends IntKeyHashMap<V> implements Map<Integer, V> {

    public OrderedIntKeyHashMap() {
        this(8);
    }

    public OrderedIntKeyHashMap(int initialCapacity) throws IllegalArgumentException {

        super(initialCapacity);

        isList = true;
    }

    public int getKeyAt(int lookup, int def) {

        if (lookup >= 0 && lookup < size()) {
            return this.intKeyTable[lookup];
        }

        return def;
    }

    public Object getValueAt(int index) {

        checkRange(index);

        return this.objectValueTable[index];
    }

    public Object setValueAt(int index, Object value) {

        checkRange(index);

        Object oldValue = objectValueTable[index];

        objectValueTable[index] = value;

        return oldValue;
    }

    public boolean set(int index, int key, V value) throws IndexOutOfBoundsException {

        checkRange(index);

        if (keySet().contains(key) && getIndex(key) != index) {
            return false;
        }
        super.remove(intKeyTable[index], 0, null, null, false, false);
        put(key, value);

        return true;
    }

    public boolean insert(int index, int key, V value) throws IndexOutOfBoundsException {

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

    public boolean setKeyAt(int index, int key) throws IndexOutOfBoundsException {

        checkRange(index);

        V value = (V) objectValueTable[index];

        return set(index, key, value);
    }

    public int getIndex(int key) {
        return getLookup(key);
    }

    public V remove(int key) {
        return (V) super.remove(key, 0, null, null, false, true);
    }

    public void removeEntry(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        int key = intKeyTable[index];

        super.remove(key, 0, null, null, false, true);
    }

    private void checkRange(int i) {

        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
    }
}
