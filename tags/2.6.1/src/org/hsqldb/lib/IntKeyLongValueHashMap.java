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
 * A Map of int primitive keys to long primitive values.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.7.2
 */
public class IntKeyLongValueHashMap extends BaseHashMap implements Map<Integer, Long> {

    private Set<Integer>              keySet;
    private Collection<Long>          values;
    private Set<Entry<Integer, Long>> entries;


    public IntKeyLongValueHashMap() {
        this(8);
    }

    public IntKeyLongValueHashMap(int initialCapacity) throws
        IllegalArgumentException {
        super(initialCapacity, BaseHashMap.intKeyOrValue,
              BaseHashMap.longKeyOrValue, false);
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

    public boolean containsValue(int value) {
        return super.containsValue(value);
    }

    public Long get(Object key) {

        if (key instanceof Integer) {

            int intKey = ((Integer) key).intValue();

            try {
                long longVal = get(intKey);

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

    public long get(int key) throws NoSuchElementException {

        int lookup = getLookup(key);

        if (lookup != -1) {
            return longValueTable[lookup];
        }

        throw new NoSuchElementException();
    }

    public long get(int key, long defaultValue) {

        int lookup = getLookup(key);

        if (lookup != -1) {
            return longValueTable[lookup];
        }

        return defaultValue;
    }

    public boolean get(int key, long[] value) {

        int lookup = getLookup(key);

        if (lookup != -1) {
            value[0] = longValueTable[lookup];

            return true;
        }

        return false;
    }

    public Long put(Integer key, Long value) {

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

    public boolean put(int key, long value) {

        int oldSize = size();

        super.addOrUpdate(key, value, null, null);

        return oldSize != size();
    }

    public Long remove(Object key) {
        if (key instanceof Integer) {

            int intKey = ((Integer) key).intValue();

            return (Long)super.remove(intKey, 0, null, null, false, false);
        }

        if (key == null) {
            throw new NullPointerException();
        }

        return null;
    }

    public boolean remove(int key) {

        Long value = (Long)super.remove(key, 0, null, null, false, false);

        return value == null ? false : true;
    }

    public void putAll(Map<? extends Integer, ? extends Long> other) {

        Iterator<? extends Integer> it = other.keySet().iterator();

        while (it.hasNext()) {
            Integer key = it.next();
            int intKey = key.intValue();
            Long value = other.get(key);
            long longValue = value.longValue();

            put(intKey, longValue);
        }
    }

    public void putAll(IntKeyIntValueHashMap other) {

        PrimitiveIterator it = (PrimitiveIterator) other.keySet().iterator();

        while (it.hasNext()) {

            int intKey = it.nextInt();
            int intValue = other.get(intKey);

            put(intKey, intValue);
        }
    }

    public int[] keysToArray(int[] array) {
        return toIntArray(array, true);
    }

    public long[] valuesToArray(long[] array) {
        return toLongArray(array, false);
    }

    public Set<Integer> keySet() {
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

    public Set<Map.Entry<Integer, Long>> entrySet() {
        if (entries == null) {
            entries = new EntrySet();
        }

        return entries;
    }

    private class EntrySet extends AbstractReadOnlyCollection<Map.Entry<Integer, Long>> implements Set<Map.Entry<Integer, Long>> {

        public Iterator<Map.Entry<Integer, Long>> iterator() {
            return IntKeyLongValueHashMap.this.new EntrySetIterator();
        }

        public int size() {
            return IntKeyLongValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }

    private class EntrySetIterator extends BaseHashIterator{

        EntrySetIterator() {
            super(true);
        }

        public Entry<Integer, Long> next() {
            Integer key   = super.nextInt();
            Long    value = longValueTable[lookup];

            return new MapEntry(key, value);
        }
    }

    private class KeySet extends AbstractReadOnlyCollection<Integer> implements Set<Integer> {

        public PrimitiveIterator<Integer> iterator() {
            return IntKeyLongValueHashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return IntKeyLongValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }

    private class Values extends AbstractReadOnlyCollection<Long> {

        public PrimitiveIterator<Long> iterator() {
            return IntKeyLongValueHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return IntKeyLongValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }
}
