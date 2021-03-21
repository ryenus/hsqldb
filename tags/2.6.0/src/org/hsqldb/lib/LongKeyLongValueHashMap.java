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

import org.hsqldb.map.BaseHashMap;

/**
 * A Map of long primitives to to long primitive.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.7.2
 */
public class LongKeyLongValueHashMap extends BaseHashMap implements Map<Long, Long> {

    private Set <Long>             keySet;
    private Collection<Long>       values;
    private Set<Entry<Long, Long>> entries;

    public LongKeyLongValueHashMap() {
        this(8);
    }

    public LongKeyLongValueHashMap(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.longKeyOrValue,
              BaseHashMap.longKeyOrValue, false);
    }

    public LongKeyLongValueHashMap(boolean minimize) {

        this(8);

        minimizeOnEmpty = minimize;
    }

    public boolean containsKey(Object key) {

        if (key instanceof Long) {

            long longKey = ((Long) key).longValue();

            return super.containsKey(longKey);
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
        if (value instanceof Long) {

            long longValue = ((Long) value).longValue();

            return super.containsValue(longValue);
        }

        if (value == null) {
            throw new NullPointerException();
        }

        return false;
    }

    public boolean containsValue(long value) {
        return super.containsValue(value);
    }

    public Long get(Object key) {

        if (key instanceof Long) {

            long longKey = ((Long) key).intValue();

            try {
                long longVal = get(longKey);

                return Long.valueOf(longVal);
            } catch (NoSuchElementException e) {
                return null;
            }
        }

        if (key == null) {
            throw new NullPointerException();
        }

        return null;
    }

    public long get(long key) throws NoSuchElementException {

        int lookup = getLookup(key);

        if (lookup != -1) {
            return longValueTable[lookup];
        }

        throw new NoSuchElementException();
    }

    public long get(long key, long defaultValue) {

        int lookup = getLookup(key);

        if (lookup != -1) {
            return longValueTable[lookup];
        }

        return defaultValue;
    }

    public boolean get(long key, long[] value) {

        int lookup = getLookup(key);

        if (lookup != -1) {
            value[0] = longValueTable[lookup];

            return true;
        }

        return false;
    }

    public Long put(Long key, Long value) {

        if (key == null || value == null) {
            throw new NullPointerException();
        }

        int oldSize = size();

        super.addOrUpdate(key, value, null, null);

        if (oldSize == size()) {
            return null;
        } else {
            return value;
        }
    }

    public boolean put(long key, long value) {

        int oldSize = size();

        super.addOrUpdate(key, value, null, null);

        return oldSize != size();
    }

    public Long remove(Object key) {
        if (key instanceof Long) {

            long longKey = ((Long) key).longValue();

            return (Long) super.remove(longKey, 0, null, null, false, false);
        }

        if (key == null) {
            throw new NullPointerException();
        }

        return null;
    }

    public boolean remove(long key) {

        Long value = (Long) super.remove(key, 0, null, null, false, false);

        return value == null ? false : true;
    }

    public void putAll(Map<? extends Long, ? extends Long> other) {

        Iterator<? extends Long> it = other.keySet().iterator();

        while (it.hasNext()) {

            Long key       = it.next();
            long longKey   = key.longValue();
            Long value     = other.get(key);
            long longValue = value.longValue();

            put(longKey, longValue);
        }
    }

    public void putAll(IntKeyIntValueHashMap other) {

        PrimitiveIterator it = (PrimitiveIterator) other.keySet().iterator();

        while (it.hasNext()) {

            long longKey   = it.nextLong();
            int  longValue = other.get(longKey);

            put(longKey, longValue);
        }
    }

    public long[] keysToArray(long[] array) {
        return toLongArray(array, true);
    }

    public long[] valuesToArray(long[] array) {
        return toLongArray(array, false);
    }

    public Set<Long> keySet() {

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

    public Set<Entry<Long, Long>> entrySet() {
        if (entries == null) {
            entries = new EntrySet();
        }

        return entries;
    }

    private class EntrySet extends AbstractReadOnlyCollection<Entry<Long, Long>> implements Set<Entry<Long, Long>> {

        public Iterator<Entry<Long, Long>> iterator() {
            return LongKeyLongValueHashMap.this.new EntrySetIterator();
        }

        public int size() {
            return LongKeyLongValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }

    private class EntrySetIterator extends BaseHashIterator{

        EntrySetIterator() {
            super(true);
        }

        public Entry<Long, Long> next() {
            Long key   = super.nextLong();
            Long value = longValueTable[lookup];

            return new MapEntry(key, value);
        }
    }

    private class KeySet<Long> extends AbstractReadOnlyCollection<Long> implements Set<Long> {

        public PrimitiveIterator<Long> iterator() {
            return LongKeyLongValueHashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return LongKeyLongValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
   }

    private class Values<Long> extends AbstractReadOnlyCollection<Long> {

        public PrimitiveIterator<Long> iterator() {
            return LongKeyLongValueHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return LongKeyLongValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }
}
