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
 * A Map of Object keys to Object values which stores multiple values per
 * key. The getValuesIterator(K key) method returns an iterator covering the
 * values associated with the given key. The get(K key) method returns the first
 * value (if any) associated with the key.<p>
 *
 * This class does not store null keys.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.9.0
 */
public class MultiValueHashMap<K, V> extends BaseHashMap implements Map<K, V>{

    private Set<K>           keySet;
    private Collection<V>    values;
    private Set<Entry<K, V>> entries;

    public MultiValueHashMap() {
        this(8);
    }

    public MultiValueHashMap(int initialCapacity) throws IllegalArgumentException {

        super(initialCapacity, BaseHashMap.objectKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);
        this.isMultiValue = true;
    }

    public MultiValueHashMap(int initialCapacity, ObjectComparator comparator) throws IllegalArgumentException {

        this(initialCapacity);
        this.comparator = comparator;
    }

    public boolean containsKey(Object key) {

        if (key == null) {
            throw new NullPointerException();
        }

        return super.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return super.containsValue(value);
    }

    /**
     * Returns one of the values associated with the given key.
     *
     * @param key the key
     * @return any value associated with the key, or null if none
     */
    public V get(Object key) {

        if (key == null) {
            throw new NullPointerException();
        }

        int hash   = comparator.hashCode(key);
        int lookup = getLookup(key, hash);

        if (lookup != -1) {
            return (V) objectValueTable[lookup];
        }

        return null;
    }

    /**
     * Returns an iterator on all values associated with the key.
     *
     * @param key the key
     * @return iterator on value associated with the key
     */
    public Iterator<V> getValuesIterator(Object key) {

        if (key == null) {
            throw new NullPointerException();
        }

        return super.getMultiValuesIterator(key);
    }

    public V put(K key, V value) {

        if (key == null) {
            throw new NullPointerException();
        }

        boolean added = super.addMultiVal(0, 0, key, value);

        return added ? null : value;
    }

    /**
     * Removes all values associated with the key.
     *
     * @param key the key
     * @return any value associated with the key, or null if none
     */
    public V remove(Object key) {

        if (key == null) {
            throw new NullPointerException();
        }

        return (V) super.removeMultiVal(0, 0, key, null, false);
    }

    /**
     * Removes the spacific value associated with the key.
     *
     * @param key the key
     * @param value the value
     * @return the value associated with the key, or null if none
     */
    public boolean remove(Object key, Object value) {

        if (key == null) {
            throw new NullPointerException();
        }

        Object result = super.removeMultiVal(0, 0, key, value, true);

        return result != null;
    }

    /**
     * Counts the values associated with the key.
     *
     * @param key the key
     * @return the count
     */
    public int valueCount(Object key) {

        if (key == null) {
            throw new NullPointerException();
        }

        int hash = comparator.hashCode(key);

        return super.multiValueElementCount(key);
    }

    public void putAll(Map<? extends K, ? extends V> m) {

        Iterator<? extends K> it = m.keySet().iterator();

        while (it.hasNext()) {
            K key = it.next();

            if (key == null) {
                continue;
            }

            put(key, m.get(key));
        }
    }

    public void putAll(MultiValueHashMap<K, V> m) {

        Iterator<K> it = m.keySet().iterator();

        while (it.hasNext()) {
            K key = it.next();

            Iterator<V> valueSet = m.getValuesIterator(key);

            while(valueSet.hasNext()) {
                V value = valueSet.next();
                put(key, value);

            }
        }
    }

    public <T> T[] keysToArray(T[] array) {
        return toArray(array, true);
    }

    public <T> T[] valuesToArray(T[] array) {
        return toArray(array, false);
    }

    public Set<K> keySet() {

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

    public Set<Entry<K, V>> entrySet() {
        if (entries == null) {
            entries = new EntrySet();
        }

        return entries;
    }

    private class EntrySet extends AbstractReadOnlyCollection<Entry<K, V>> implements Set<Map.Entry<K, V>> {

        public Iterator<Entry<K, V>> iterator() {
            return MultiValueHashMap.this.new EntrySetIterator();
        }

        public int size() {
            return MultiValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }

    private class EntrySetIterator extends BaseHashIterator{

        EntrySetIterator() {
            super(true);
        }

        public Entry<K, V> next() {
            K key   = (K) super.next();
            V value = (V) objectValueTable[lookup];

            return new MapEntry(key, value);
        }
    }

    private class KeySet<K> extends AbstractReadOnlyCollection<K> implements Set<K> {

        public Iterator<K> iterator() {
            return MultiValueHashMap.this.new MultiValueKeyIterator();
        }

        public int size() {
            return MultiValueHashMap.this.multiValueKeyCount();
        }

        public boolean contains(Object key) {
            return containsKey(key);
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public Object[] toArray() {
            return MultiValueHashMap.this.toArray(true);
        }

        public <T> T[] toArray(T[] array) {
            return MultiValueHashMap.this.multiValueKeysToArray(array);
        }
    }

    private class Values<V> extends AbstractReadOnlyCollection<V> {

        public Iterator<V> iterator() {
            return MultiValueHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return MultiValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public Object[] toArray() {
            return MultiValueHashMap.this.toArray(false);
        }

        public <T> T[] toArray(T[] array) {
            return MultiValueHashMap.this.toArray(array, false);
        }
    }
}
