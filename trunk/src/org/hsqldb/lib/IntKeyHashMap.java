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
 * A Map of int primitives to Object values.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.7.2
 */
public class IntKeyHashMap<V> extends BaseHashMap implements Map<Integer, V> {

    private Set<Integer>           keySet;
    private Collection<V>          values;
    private Set<Entry<Integer, V>> entries;

    public IntKeyHashMap() {
        this(8);
    }

    public IntKeyHashMap(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.intKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);
    }

    public boolean containsKey(Object key) {

        if (key instanceof Integer) {

            int intKey = ((Integer) key).intValue();

            return containsKey(intKey);
        }

        if (key == null) {
            throw new NullPointerException();
        }

        return false;
    }

    public boolean containsKey(int key) {
        return super.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return super.containsValue(value);
    }

    public V get(Object key) {

        if (key instanceof Integer) {

            int intKey = ((Integer) key).intValue();

            return get(intKey);
        }

        if (key == null) {
            throw new NullPointerException();
        }

        return null;
    }

    public V get(int key) {

        int lookup = getLookup(key);

        if (lookup != -1) {
            return (V) objectValueTable[lookup];
        }

        return null;
    }

    public V put(Integer key, V value) {

        if (key == null) {
            throw new NullPointerException();
        }

        int intKey = ((Integer) key).intValue();

        return put(intKey, value);
    }

    public V put(int key, V value) {
        return (V) super.addOrUpdate(key, 0, null, value);
    }

    public V remove(Object key) {
        if (key instanceof Integer) {

            int intKey = ((Integer) key).intValue();

            return remove(intKey);
        }

        if (key == null) {
            throw new NullPointerException();
        }

        return null;
    }

    public V remove(int key) {
        return (V) super.remove(key, 0, null, null, false, false);
    }

    public void putAll(Map<? extends Integer, ? extends V> other) {
        Iterator<? extends Integer> it = other.keySet().iterator();

        while (it.hasNext()) {
            Integer key = it.next();

            if (key == null) {
                continue;
            }

            int intKey = key.intValue();

            put(intKey, (V) other.get(key));
        }
    }

    public void putAll(IntKeyHashMap other) {

        PrimitiveIterator it = (PrimitiveIterator) other.keySet().iterator();

        while (it.hasNext()) {
            int key = it.nextInt();

            put(key, (V) other.get(key));
        }
    }

    public int[] keysToArray(int[] array) {
        return toIntArray(array, true);
    }

    public Object[] valuesToArray() {

        return toArray(false);
    }

    public <T> T[] valuesToArray(T[] array) {
        return toArray(array, false);
    }

    public Set<Integer> keySet() {

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

    public Set<Entry<Integer, V>> entrySet() {
        if (entries == null) {
            entries = new EntrySet();
        }

        return entries;
    }

    private class EntrySet extends AbstractReadOnlyCollection<Map.Entry<Integer, V>> implements Set<Map.Entry<Integer, V>> {

        public Iterator<Entry<Integer, V>> iterator() {
            return IntKeyHashMap.this.new EntrySetIterator();
        }

        public int size() {
            return IntKeyHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }

    private class EntrySetIterator extends BaseHashIterator{

        EntrySetIterator() {
            super(true);
        }

        public Entry<Integer, V> next() {
            Integer key   = super.nextInt();
            V value       = (V) objectValueTable[lookup];

            return new MapEntry(key, value);
        }
    }

    private class KeySet<Integer> extends AbstractReadOnlyCollection<Integer> implements Set<Integer> {

        public PrimitiveIterator<Integer> iterator() {
            return IntKeyHashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return IntKeyHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }

    private class Values<V> extends AbstractReadOnlyCollection<V> {

        public Iterator<V> iterator() {
            return IntKeyHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return IntKeyHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public Object[] toArray() {
            return IntKeyHashMap.this.toArray(false);
        }

        public <T> T[] toArray(T[] array) {
            return IntKeyHashMap.this.toArray(array, false);
        }
    }
}
