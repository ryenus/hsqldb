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

import org.hsqldb.map.BaseHashMap;

/**
 * This class does not store null keys.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.2
 * @since 1.7.2
 */
public class HashMap<K, V> extends BaseHashMap {

    Set        keySet;
    Collection values;

    public HashMap() {
        this(8);
    }

    public HashMap(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.objectKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);
    }

    public V get(Object key) {

        int hash   = key.hashCode();
        int lookup = getLookup(key, hash);

        if (lookup != -1) {
            return (V) objectValueTable[lookup];
        }

        return null;
    }

    public V put(K key, V value) {
        return (V) super.addOrRemove(0, 0, (K) key, (V) value, false);
    }

    public V remove(Object key) {
        return (V) super.removeObject((K)key, false);
    }

    public boolean containsKey(Object key) {
        return super.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return super.containsValue(value);
    }

    public void putAll(HashMap<? extends K, ? extends V> t) {

        Iterator<? extends K> it = t.keySet().iterator();

        while (it.hasNext()) {
            K key = it.next();

            put(key, t.get(key));
        }
    }

    public void valuesToArray(V[] array) {

        Iterator<V> it = values().iterator();
        int      i  = 0;

        while (it.hasNext()) {
            array[i] = it.next();

            i++;
        }
    }

    public void keysToArray(K[] array) {

        Iterator<K> it = keySet().iterator();
        int      i  = 0;

        while (it.hasNext()) {
            array[i] = it.next();

            i++;
        }
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

    class KeySet implements Set<K> {

        public Iterator iterator() {
            return HashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return HashMap.this.size();
        }

        public boolean contains(Object key) {
            return containsKey(key);
        }

        public K get(K key) {

            int lookup = HashMap.this.getLookup(key, key.hashCode());

            if (lookup < 0) {
                return null;
            } else {
                return (K) HashMap.this.objectKeyTable[lookup];
            }
        }

        public boolean add(K key) {
            throw new UnsupportedOperationException();
        }

        public boolean addAll(Collection<? extends K> c) {
            throw new UnsupportedOperationException();
        }

        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public void clear() {
            HashMap.this.clear();
        }
    }

    class Values implements Collection<V> {

        public Iterator iterator() {
            return HashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return HashMap.this.size();
        }

        public boolean contains(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean add(V value) {
            throw new UnsupportedOperationException();
        }

        public boolean addAll(Collection<? extends V> c) {
            throw new UnsupportedOperationException();
        }

        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public void clear() {
            HashMap.this.clear();
        }
    }
}
