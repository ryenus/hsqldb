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

import org.hsqldb.map.BaseHashMap;

/**
 * A Map of long primitives to Object values which maintains the insertion order
 * of the key/value pairs and allows access by index. Iterators return the keys
 * or values in the index order.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.9.0
 */
public class OrderedLongKeyHashMap<V> extends BaseHashMap implements Map<Long, V> {

    private Set<Long>                 keySet;
    private Collection<V>             values;
    private Set<Entry<Long, V>> entries;

    public OrderedLongKeyHashMap() {
        this(8);
    }

    public OrderedLongKeyHashMap(int initialCapacity)
    throws IllegalArgumentException {

        super(initialCapacity, BaseHashMap.longKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);

        isList = true;
    }

    public OrderedLongKeyHashMap(int initialCapacity,
                                 boolean hasThirdValue)
                                 throws IllegalArgumentException {

        super(initialCapacity, BaseHashMap.longKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);

        objectKeyTable   = new Object[objectValueTable.length];
        isTwoObjectValue = true;
        isList           = true;

        if (hasThirdValue) {
            objectValueTable2 = new Object[objectValueTable.length];
        }

        minimizeOnEmpty = true;
    }

    public boolean containsKey(Object key) {

        if (key instanceof Long) {

            long longKey = ((Long) key).longValue();

            return containsKey(longKey);
        }

        if (key == null) {
            throw new NullPointerException();
        }

        return false;
    }

    public boolean containsKey(long key) {
        return super.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return super.containsValue(value);
    }

    public V get(Object key) {

        if (key instanceof Long) {

            long longKey = ((Long) key).longValue();

            return get(longKey);
        }

        if (key == null) {
            throw new NullPointerException();
        }

        return null;
    }

    public V get(long key) {

        int lookup = getLookup(key);

        if (lookup != -1) {
            return (V) objectValueTable[lookup];
        }

        return null;
    }

    public long getKeyAt(int index) {
        checkRange(index);

        return longKeyTable[index];
    }

    public V getValueAt(int index) {
        checkRange(index);

        return (V) objectValueTable[index];
    }

    public Object getSecondValueAt(int index) {
        checkRange(index);

        return objectKeyTable[index];
    }

    public Object getThirdValueAt(int index) {
        checkRange(index);

        return objectValueTable2[index];
    }

    public Object setValueAt(int index, Object value) {

        checkRange(index);

        Object oldValue = objectValueTable[index];

        objectValueTable[index] = value;

        return oldValue;
    }

    public Object setSecondValueAt(int index, Object value) {

        checkRange(index);

        Object oldValue = objectKeyTable[index];

        objectKeyTable[index] = value;

        return oldValue;
    }

    public Object setThirdValueAt(int index, Object value) {

        checkRange(index);

        Object oldValue = objectValueTable2[index];

        objectValueTable2[index] = value;

        return oldValue;
    }

    public boolean insert(int index, long key, V value) throws IndexOutOfBoundsException {

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

        put(key, value);

        return true;
    }

    public boolean set(int index, long key, V value) throws IndexOutOfBoundsException {

        checkRange(index);

        if (keySet().contains(key) && getIndex(key) != index) {
            return false;
        }

        super.remove(longKeyTable[index], 0, null, null, false, false);
        put(key, value);

        return true;
    }

    public boolean setKeyAt(int index, long key) throws IndexOutOfBoundsException {

        checkRange(index);

        V value = (V) objectValueTable[index];

        return set(index, key, value);
    }

    public int getIndex(long key) {
        return getLookup(key);
    }

    public V put(Long key, V value) {

        if (key == null) {
            throw new NullPointerException();
        }

        long longKey = ((Long) key).longValue();

        return put(longKey, value);
    }

    public V put(long key, V value) {
        return (V) super.addOrUpdate(key, 0, null, value);
    }

    public V remove(Object key) {
        if (key instanceof Long) {

            long longKey = ((Long) key).longValue();

            return remove(longKey);
        }

        if (key == null) {
            throw new NullPointerException();
        }

        return null;
    }

    public V remove(long key) {

        V returnValue = (V) super.remove(key, 0, null, null, false, true);

        return returnValue;
    }

    public void removeEntry(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        long key = longKeyTable[index];

        super.remove(key, 0, null, null, false, true);
    }

    public int getLookup(long key) {
        return super.getLookup(key);
    }

    public void putAll(Map<? extends Long, ? extends V> other) {
        Iterator<? extends Long> it = other.keySet().iterator();

        while (it.hasNext()) {
            Long key = it.next();
            long longKey = key.longValue();

            put(longKey, (V) other.get(key));
        }
    }

    public void putAll(LongKeyHashMap other) {

        PrimitiveIterator it = (PrimitiveIterator) other.keySet().iterator();

        while (it.hasNext()) {
            long key = it.nextLong();

            put(key, (V) other.get(key));
        }
    }

    public long[] keysToArray(long[] array) {

        return toLongArray(array, true);
    }

    public Object[] valuesToArray() {

        return toArray(false);
    }

    public <T> T[] valuesToArray(T[] array) {

        return toArray(array, false);
    }

    public Set<Long> keySet() {

        if (keySet == null) {
            keySet = new KeySet();
        }

        return keySet;
    }

    public Collection<V> values() {

        if (values == null) {
            values = new Values();
        }

        return values;
    }

    public Set<Entry<Long, V>> entrySet() {
        if (entries == null) {
            entries = new EntrySet();
        }

        return entries;
    }

    private class EntrySet extends AbstractReadOnlyCollection<Entry<Long, V>> implements Set<Entry<Long, V>> {

        public Iterator<Entry<Long, V>> iterator() {
            return OrderedLongKeyHashMap.this.new EntrySetIterator();
        }

        public int size() {
            return OrderedLongKeyHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }

    private class EntrySetIterator extends BaseHashIterator{

        EntrySetIterator() {
            super(true);
        }

        public Entry<Long, V> next() {
            Long key   = super.nextLong();
            V    value = (V) objectValueTable[lookup];

            return new MapEntry(key, value);
        }
    }

    private void checkRange(int i) {

        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
    }

   private  class KeySet<Long> extends AbstractReadOnlyCollection<Long> implements Set<Long> {

        public PrimitiveIterator<Long> iterator() {
            return OrderedLongKeyHashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return OrderedLongKeyHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }

    private class Values<V> extends AbstractReadOnlyCollection<V> {

        public PrimitiveIterator<V> iterator() {
            return OrderedLongKeyHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return OrderedLongKeyHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public Object[] toArray() {
            return OrderedLongKeyHashMap.this.toArray(false);
        }

        public <T> T[] toArray(T[] array) {
            return OrderedLongKeyHashMap.this.toArray(array, false);
        }
    }
}
