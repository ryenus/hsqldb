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

import java.util.NoSuchElementException;

import org.hsqldb.lib.Map.Entry;
import org.hsqldb.map.BaseHashMap;

/**
 * A Map of Object keys to long primitives.<p>
 *
 * This class does not store null keys.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.9.0
 */
public class LongValueHashMap<K> extends BaseHashMap {

    private Set<K>              keySet;
    private Collection<Long>    values;
    private Set<Entry<K, Long>> entries;

    public LongValueHashMap() {
        this(8);
    }

    public LongValueHashMap(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.objectKeyOrValue,
              BaseHashMap.longKeyOrValue, false);
    }

    public LongValueHashMap(int initialCapacity, ObjectComparator comparator) {

        this(initialCapacity);

        this.comparator = comparator;
    }

    public long get(Object key) throws NoSuchElementException {

        if (key == null) {
            throw new NullPointerException();
        }

        int lookup = getLookup(key);

        if (lookup != -1) {
            return longValueTable[lookup];
        }

        throw new NoSuchElementException();
    }

    public long get(Object key, int defaultValue) {

        if (key == null) {
            throw new NullPointerException();
        }

        int lookup = getLookup(key);

        if (lookup != -1) {
            return longValueTable[lookup];
        }

        return defaultValue;
    }

    public boolean get(Object key, long[] value) {

        if (key == null) {
            throw new NullPointerException();
        }

        int lookup = getLookup(key);

        if (lookup != -1) {
            value[0] = longValueTable[lookup];

            return true;
        }

        return false;
    }

    public Object getKey(long value) {

        BaseHashIterator it = new BaseHashIterator(false);

        while (it.hasNext()) {
            long i = it.nextLong();

            if (i == value) {
                return objectKeyTable[it.getLookup()];
            }
        }

        return null;
    }

    public boolean put(Object key, long value) {

        if (key == null) {
            throw new NullPointerException();
        }

        int oldSize = size();

        super.addOrUpdate(0, value, key, null);

        return oldSize != size();
    }

    public boolean remove(Object key) {

        if (key == null) {
            throw new NullPointerException();
        }

        int oldSize = size();

        super.remove(0, 0, key, null, false, false);

        return oldSize != size();
    }

    public boolean containsKey(Object key) {
        if (key == null) {
            throw new NullPointerException();
        }

        return super.containsKey(key);
    }

    public void putAll(LongValueHashMap t) {

        Iterator it = t.keySet().iterator();

        while (it.hasNext()) {
            Object key = it.next();

            put(key, t.get(key));
        }
    }

    public Set<K> keySet() {

        if (keySet == null) {
            keySet = new KeySet();
        }

        return keySet;
    }

    public Collection<Long> values() {

        if (values == null) {
            values = new Values();
        }

        return values;
    }

    public Set<Entry<K, Long>> entrySet() {
        if (entries == null) {
            entries = new EntrySet();
        }

        return entries;
    }

    private class EntrySet extends AbstractReadOnlyCollection<Entry<K, Long>> implements Set<Entry<K, Long>> {

        public Iterator<Entry<K, Long>> iterator() {
            return LongValueHashMap.this.new EntrySetIterator();
        }

        public int size() {
            return LongValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }

    private class EntrySetIterator extends BaseHashIterator{

        EntrySetIterator() {
            super(true);
        }

        public Entry<K, Long> next() {
            K    key   = (K) super.next();
            Long value = longValueTable[lookup];

            return new MapEntry(key, value);
        }
    }

    private class KeySet<K> extends AbstractReadOnlyCollection<K> implements Set<K> {

        public Iterator<K> iterator() {
            return LongValueHashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return LongValueHashMap.this.size();
        }

        public boolean contains(Object key) {
            return containsKey(key);
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public Object[] toArray() {
            return LongValueHashMap.this.toArray(true);
        }

        public <T> T[] toArray(T[] array) {
            return LongValueHashMap.this.toArray(array, true);
        }
    }

    private class Values<Long> extends AbstractReadOnlyCollection<Long> {

        public PrimitiveIterator<Long> iterator() {
            return LongValueHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return LongValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }
}
