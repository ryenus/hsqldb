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
 * A Map of long primitives to Object values.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.9.0
 */
public class LongKeyHashMap<V> extends BaseHashMap implements Map<Long, V> {

    ReentrantReadWriteLock           lock = new ReentrantReadWriteLock(true);
    ReentrantReadWriteLock.ReadLock  readLock  = lock.readLock();
    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    //
    private Set<Long>           keySet;
    private Collection<V>       values;
    private Set<Entry<Long, V>> entries;

    public LongKeyHashMap() {
        this(16);
    }

    public LongKeyHashMap(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.longKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);
    }

    public Lock getReadLock() {
        return readLock;
    }

    public Lock getWriteLock() {
        return writeLock;
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

        readLock.lock();

        try {
            return super.containsKey(key);
        } finally {
            readLock.unlock();
        }
    }

    public boolean containsValue(Object value) {

        readLock.lock();

        try {
            return super.containsValue(value);
        } finally {
            readLock.unlock();
        }
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

        readLock.lock();

        try {
            int lookup = getLookup(key);

            if (lookup == -1) {
                return null;
            }

            return (V) objectValueTable[lookup];
        } finally {
            readLock.unlock();
        }
    }

    public V put(Long key, V value) {

        if (key == null) {
            throw new NullPointerException();
        }

        long longKey = ((Long) key).longValue();

        return put(longKey, value);
    }

    public V put(long key, V value) {

        writeLock.lock();

        try {
            return (V) super.addOrUpdate(key, 0, null, value);
        } finally {
            writeLock.unlock();
        }
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

        writeLock.lock();

        try {
            return (V) super.remove(key, 0, null, null, false, false);
        } finally {
            writeLock.unlock();
        }
    }

    public void clear() {

        writeLock.lock();

        try {
            super.clear();
        } finally {
            writeLock.unlock();
        }
    }

    public boolean isEmpty() {

        readLock.lock();

        try {
            return super.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    public int size() {

        readLock.lock();

        try {
            return super.size();
        } finally {
            readLock.unlock();
        }
    }

    public void putAll(Map<? extends Long, ? extends V> other) {
        Iterator<? extends Long> it = other.keySet().iterator();

        writeLock.lock();

        try {
            while (it.hasNext()) {
                Long key = it.next();
                long longKey = key.longValue();

                put(longKey, (V) other.get(key));
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void putAll(LongKeyHashMap other) {

        writeLock.lock();

        try {
            PrimitiveIterator it = (PrimitiveIterator) other.keySet().iterator();

            while (it.hasNext()) {
                long key = it.nextLong();

                put(key, (V) other.get(key));
            }
        } finally {
            writeLock.unlock();
        }
    }

    public long[] keysToArray(long[] array) {

        readLock.lock();

        try {
            return toLongArray(array, true);
        } finally {
            readLock.unlock();
        }
    }

    public Object[] valuesToArray() {

        readLock.lock();

        try {
            return toArray(false);
        } finally {
            readLock.unlock();
        }
    }

    public <T> T[] valuesToArray(T[] array) {

        readLock.lock();

        try {
            return toArray(array, false);
        } finally {
            readLock.unlock();
        }
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

    public Set<Map.Entry<Long, V>> entrySet() {
        if (entries == null) {
            entries = new EntrySet();
        }

        return entries;
    }

    private class EntrySet extends AbstractReadOnlyCollection<Map.Entry<Long, V>> implements Set<Map.Entry<Long, V>> {

        public Iterator<Entry<Long, V>> iterator() {
            return LongKeyHashMap.this.new EntrySetIterator();
        }

        public int size() {
            return LongKeyHashMap.this.size();
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

    private class KeySet<Long> extends AbstractReadOnlyCollection<Long> implements Set<Long> {

        public PrimitiveIterator<Long> iterator() {
            return LongKeyHashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return LongKeyHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }

    private class Values<V> extends AbstractReadOnlyCollection<V> {

        public Iterator<V> iterator() {
            return LongKeyHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return LongKeyHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public Object[] toArray() {
            return LongKeyHashMap.this.toArray(false);
        }

        public <T> T[] toArray(T[] a) {
            return LongKeyHashMap.this.toArray(a, false);
        }
    }
}
