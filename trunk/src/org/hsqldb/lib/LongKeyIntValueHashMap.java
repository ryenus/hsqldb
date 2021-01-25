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
 * A Map of long primitives to int primitives.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.7.2
 */
public class LongKeyIntValueHashMap extends BaseHashMap implements Map<Long, Integer> {

    private Set<Long>                 keySet;
    private Collection<Integer>       values;
    private Set<Entry<Long, Integer>> entries;

    public LongKeyIntValueHashMap() {
        this(8);
    }

    public LongKeyIntValueHashMap(boolean minimize) {

        this(8);

        minimizeOnEmpty = minimize;
    }

    public LongKeyIntValueHashMap(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.longKeyOrValue,
              BaseHashMap.intKeyOrValue, false);
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
        if (value instanceof Integer) {

            int intValue = ((Integer) value).intValue();

            return super.containsValue(intValue);
        }

        if (value == null) {
            throw new NullPointerException();
        }

        return false;
    }

    public boolean containsValue(int value) {
        return super.containsValue(value);
    }

    public Integer get(Object key) {

        if (key instanceof Long) {

            long longKey = ((Long) key).longValue();

            try {
                int intVal = get(longKey);

                return Integer.valueOf(intVal);
            } catch (NoSuchElementException e) {
                return null;
            }
        }

        if (key == null) {
            throw new NullPointerException();
        }

        return null;
    }

    public int get(long key) throws NoSuchElementException {

        int lookup = getLookup(key);

        if (lookup != -1) {
            return intValueTable[lookup];
        }

        throw new NoSuchElementException();
    }

    public int get(long key, int defaultValue) {

        int lookup = getLookup(key);

        if (lookup != -1) {
            return intValueTable[lookup];
        }

        return defaultValue;
    }

    public boolean get(long key, int[] value) {

        int lookup = getLookup(key);

        if (lookup != -1) {
            value[0] = intValueTable[lookup];

            return true;
        }

        return false;
    }

    public int getLookup(long key) {
        return super.getLookup(key);
    }

    public Integer put(Long key, Integer value) {

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

    public boolean put(long key, int value) {

        int oldSize = size();

        super.addOrUpdate(key, value, null, null);

        return oldSize != size();
    }

    public Integer remove(Object key) {
        if (key instanceof Long) {

            long longKey = ((Long) key).longValue();

            return (Integer) super.remove(longKey, 0, null, null, false, false);
        }

        if (key == null) {
            throw new NullPointerException();
        }

        return null;
    }

    public boolean remove(long key) {

        Integer value = (Integer) super.remove(key, 0, null, null, false, false);

        return value == null ? false : true;
    }


    public void putAll(Map<? extends Long, ? extends Integer> other) {

        Iterator<? extends Long> it = other.keySet().iterator();

        while (it.hasNext()) {
            Long    key      = it.next();
            long    longKey  = key.longValue();
            Integer value    = other.get(key);
            int     intValue = value.intValue();

            put(longKey, intValue);
        }
    }

    public void putAll(LongKeyIntValueHashMap other) {

        PrimitiveIterator it = (PrimitiveIterator) other.keySet().iterator();

        while (it.hasNext()) {

            long key   = it.nextLong();
            int  value = other.get(key);

            put(key, value);
        }
    }

    public long[] keysToArray(long[] array) {
        return toLongArray(array, true);
    }

    public int[] valuesToArray(int[] array) {
        return toIntArray(array, false);
    }

    public Set<Long> keySet() {

        if (keySet == null) {
            keySet = new KeySet();
        }

        return keySet;
    }

    public Collection<Integer> values() {

        if (values == null) {
            values = new Values();
        }

        return values;
    }

    public Set<Entry<Long, Integer>> entrySet() {
        if (entries == null) {
            entries = new EntrySet();
        }

        return entries;
    }

    private class EntrySet extends AbstractReadOnlyCollection<Entry<Long, Integer>> implements Set<Entry<Long, Integer>> {

        public Iterator<Entry<Long, Integer>> iterator() {
            return LongKeyIntValueHashMap.this.new EntrySetIterator();
        }

        public int size() {
            return LongKeyIntValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }

    private class EntrySetIterator extends BaseHashIterator{

        EntrySetIterator() {
            super(true);
        }

        public Entry<Long, Integer> next() {
            Long    key   = super.nextLong();
            Integer value = intValueTable[lookup];

            return new MapEntry(key, value);
        }
    }

    private class KeySet<Long> extends AbstractReadOnlyCollection<Long> implements Set<Long> {

        public PrimitiveIterator<Long> iterator() {
            return LongKeyIntValueHashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return LongKeyIntValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }

    private class Values<Integer> extends AbstractReadOnlyCollection<Integer> {

        public PrimitiveIterator<Integer> iterator() {
            return LongKeyIntValueHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return LongKeyIntValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }
}
