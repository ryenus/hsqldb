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
 * A Map of Object keys to int primitive values.<p>
 *
 * This class does not store null keys.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.7.2
 */
public class IntValueHashMap<K> extends BaseHashMap {

    private Set<K>                 keySet;
    private Collection<Integer>    values;
    private Set<Entry<K, Integer>> entries;

    public IntValueHashMap() {
        this(8);
    }

    public IntValueHashMap(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.objectKeyOrValue,
              BaseHashMap.intKeyOrValue, false);
    }

    public boolean containsKey(Object key) {
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

    public int get(Object key) throws NoSuchElementException {

        if (key == null) {
            throw new NullPointerException();
        }

        int lookup = getLookup(key);

        if (lookup != -1) {
            return intValueTable[lookup];
        }

        throw new NoSuchElementException();
    }

    public int get(Object key, int defaultValue) {

        if (key == null) {
            throw new NullPointerException();
        }

        int lookup = getLookup(key);

        if (lookup != -1) {
            return intValueTable[lookup];
        }

        return defaultValue;
    }

    public boolean get(Object key, int[] value) {

        if (key == null) {
            throw new NullPointerException();
        }

        int lookup = getLookup(key);

        if (lookup != -1) {
            value[0] = intValueTable[lookup];

            return true;
        }

        return false;
    }

    public Object getKey(int value) {

        BaseHashIterator it = new BaseHashIterator(false);

        while (it.hasNext()) {
            int i = it.nextInt();

            if (i == value) {
                return objectKeyTable[it.getLookup()];
            }
        }

        return null;
    }

    public boolean put(Object key, int value) {

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

    public void putAll(Map<? extends K, ? extends Integer> other) {

        Iterator<? extends K> it = other.keySet().iterator();

        while (it.hasNext()) {
            K        key      = it.next();
            Integer value    = other.get(key);
            int     intValue = value.intValue();

            put(key, intValue);
        }
    }

    public void putAll(IntValueHashMap other) {

        Iterator it = other.keySet().iterator();

        while (it.hasNext()) {
            Object key = it.next();

            put(key, other.get(key));
        }
    }

    public Set<K> keySet() {

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

    public Set<Map.Entry<K, Integer>> entrySet() {
        if (entries == null) {
            entries = new EntrySet();
        }

        return entries;
    }

    private class EntrySet extends AbstractReadOnlyCollection<Entry<K, Integer>> implements Set<Entry<K, Integer>> {

        public Iterator<Entry<K, Integer>> iterator() {
            return IntValueHashMap.this.new EntrySetIterator();
        }

        public int size() {
            return IntValueHashMap.this.size();
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
            K       key   = (K) super.next();
            Integer value = intValueTable[lookup];

            return new MapEntry(key, value);
        }
    }

    class KeySet extends AbstractReadOnlyCollection<K> implements Set<K> {

        public PrimitiveIterator<K> iterator() {
            return IntValueHashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return IntValueHashMap.this.size();
        }

        public boolean contains(Object o) {
            return containsKey(o);
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public Object[] toArray() {
            return IntValueHashMap.this.toArray(true);
        }

        public <T> T[] toArray(T[] a) {
            return IntValueHashMap.this.toArray(a, true);
        }
    }

    class Values extends AbstractReadOnlyCollection<Integer> {

        public Iterator<Integer> iterator() {
            return IntValueHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return IntValueHashMap.this.size();
        }

        public boolean isEmpty() {
            return size() == 0;
        }
    }
}
