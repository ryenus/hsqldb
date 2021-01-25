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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb.map.BaseHashMap;

/**
 * A Map of int primitives to Object values, suitable for thread-safe access.<p>
 *
 * Iterators of keys or values are not thread-safe.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.9.0
 */
public class IntKeyHashMapConcurrent<V> extends BaseHashMap implements Map<Integer, V> {

    private Set<Integer>           keySet;
    private Collection<V>          values;
    private Set<Entry<Integer, V>> entries;

    //
    ReentrantReadWriteLock           lock = new ReentrantReadWriteLock(true);
    ReentrantReadWriteLock.ReadLock  readLock  = lock.readLock();
    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    public IntKeyHashMapConcurrent() {
        this(8);
    }

    public IntKeyHashMapConcurrent(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.intKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);
    }

    public Lock getWriteLock() {
        return writeLock;
    }

    public boolean containsKey(Object key) {

        if (key instanceof Integer) {

            int intKey = ((Integer) key).intValue();

            return super.containsKey(intKey);
        }

        if (key == null) {
            throw new NullPointerException();
        }

        return false;
    }

    public boolean containsKey(int key) {

        try {
            readLock.lock();

            return super.containsKey(key);
        } finally {
            readLock.unlock();
        }
    }

    public boolean containsValue(Object value) {

        try {
            readLock.lock();

            return super.containsValue(value);
        } finally {
            readLock.unlock();
        }
    }

    public V get(Object key) {

        if (key instanceof Integer) {

            int intKey = ((Integer) key).intValue();

            return get(intKey);
        }

        return null;
    }

    public V get(int key) {

        try {
            readLock.lock();

            int lookup = getLookup(key);

            if (lookup != -1) {
                return (V) objectValueTable[lookup];
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    public V put(Integer key, V value) {

        if (key == null) {
            throw new NullPointerException();
        }

        int intKey = ((Integer) key).intValue();

        return put(intKey, value);
    }


    public V put(int key, V value) {

        try {
            writeLock.lock();

            return (V) super.addOrUpdate(key, 0, null, value);
        } finally {
            writeLock.unlock();
        }
    }

    public V remove(Object key) {
        if (key instanceof Integer) {

            int intKey = ((Integer) key).intValue();

            return remove(intKey);
        }

        return null;
    }

    public V remove(int key) {

        try {
            writeLock.lock();

            return (V) super.remove(key, 0, null, null, false, false);
        } finally {
            writeLock.unlock();
        }
    }

    public void putAll(Map<? extends Integer, ? extends V> other) {
        try {
            writeLock.lock();

            Iterator<? extends Integer> it = other.keySet().iterator();

            while (it.hasNext()) {
                Integer key = it.next();
                int intKey = key.intValue();

                put(intKey, (V) other.get(key));
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void putAll(IntKeyHashMap other) {

        try {
            writeLock.lock();

            PrimitiveIterator it = (PrimitiveIterator) other.keySet().iterator();

            while (it.hasNext()) {
                int intKey = it.nextInt();

                put(intKey, (V) other.get(intKey));
            }
        } finally {
            writeLock.unlock();
        }
    }

    public int getOrderedKeyMatchCount(int[] array) {

        int i = 0;

        try {
            readLock.lock();

            for (; i < array.length; i++) {
                if (!super.containsKey(array[i])) {
                    break;
                }
            }

            return i;
        } finally {
            readLock.unlock();
        }
    }

    public int[] keysToArray(int[] array) {
        try {
            readLock.lock();

            return toIntArray(array, true);
        } finally {
            readLock.unlock();
        }
    }

    public <T> T[] valuesToArray(T[] array) {
        try {
            readLock.lock();

            return toArray(array, false);
        } finally {
            readLock.unlock();
        }
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
            return IntKeyHashMapConcurrent.this.new EntrySetIterator();
        }

        public int size() {
            return IntKeyHashMapConcurrent.this.size();
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
            return IntKeyHashMapConcurrent.this.new BaseHashIterator(true);
        }

        public int size() {
            return IntKeyHashMapConcurrent.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }

    private class Values<V> extends AbstractReadOnlyCollection<V> {

        public Iterator<V> iterator() {
            return IntKeyHashMapConcurrent.this.new BaseHashIterator(false);
        }

        public int size() {
            return IntKeyHashMapConcurrent.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public Object[] toArray() {
            Object[] array = new Object[size()];
            return IntKeyHashMapConcurrent.this.valuesToArray(array);
        }

        public <T> T[] toArray(T[] a) {
            return IntKeyHashMapConcurrent.this.valuesToArray(a);
        }
    }
}
