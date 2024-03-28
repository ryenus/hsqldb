/* Copyright (c) 2001-2024, The HSQL Development Group
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
 * A set of int primitives.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 2.3.0
 */
public class IntHashSet extends BaseHashMap {

    public IntHashSet() {
        this(8);
    }

    public IntHashSet(int initialCapacity) throws IllegalArgumentException {

        super(
            initialCapacity,
            BaseHashMap.intKeyOrValue,
            BaseHashMap.noKeyOrValue,
            false);
    }

    public IntHashSet(int[] elements) {

        super(
            elements.length,
            BaseHashMap.intKeyOrValue,
            BaseHashMap.noKeyOrValue,
            false);

        addAll(elements);
    }

    public IntHashSet(int[] elementsA, int[] elementsB) {

        super(
            elementsA.length + elementsB.length,
            BaseHashMap.intKeyOrValue,
            BaseHashMap.noKeyOrValue,
            false);

        addAll(elementsA);
        addAll(elementsB);
    }

    public boolean contains(Object o) {

        if (o instanceof Integer) {
            int intKey = ((Integer) o).intValue();

            return containsIntKey(intKey);
        }

        return false;
    }

    public boolean contains(int key) {
        return super.containsIntKey(key);
    }

    public boolean add(Integer e) {

        if (e == null) {
            throw new NullPointerException();
        }

        int intKey = e.intValue();

        return add(intKey);
    }

    public boolean add(int key) {
        return (Boolean) super.addOrUpdate(key, 0, null, null);
    }

    public boolean remove(int key) {
        return (Boolean) super.remove(key, 0, null, null, false, false);
    }

    public int getStartMatchCount(int[] array) {

        int i = 0;

        for (; i < array.length; i++) {
            if (!super.containsIntKey(array[i])) {
                break;
            }
        }

        return i;
    }

    public boolean addAll(Collection<Integer> col) {

        int               oldSize = size();
        Iterator<Integer> it      = col.iterator();

        while (it.hasNext()) {
            add(it.next());
        }

        return oldSize != size();
    }

    public boolean addAll(IntHashSet s) {

        boolean           result = false;
        Iterator<Integer> it     = s.iterator();

        while (it.hasNext()) {
            result |= add(it.nextInt());
        }

        return result;
    }

    public boolean addAll(int[] elements) {

        int oldSize = size();

        for (int i = 0; i < elements.length; i++) {
            add(elements[i]);
        }

        return oldSize != size();
    }

    public boolean containsAll(IntHashSet c) {

        Iterator<Integer> it = c.iterator();

        while (it.hasNext()) {
            if (!contains(it.nextInt())) {
                return false;
            }
        }

        return true;
    }

    public boolean retainAll(IntHashSet c) {

        int               oldSize = size();
        Iterator<Integer> it      = iterator();

        while (it.hasNext()) {
            if (!c.contains(it.nextInt())) {
                it.remove();
            }
        }

        return oldSize != size();
    }

    public boolean removeAll(IntHashSet c) {

        int               oldSize = size();
        Iterator<Integer> it      = c.iterator();

        while (it.hasNext()) {
            int value = it.nextInt();

            remove(value);
        }

        return oldSize != size();
    }

    public int[] toArray() {
        int[] array = new int[size()];

        return toIntArray(array, true);
    }

    public int[] toArray(int[] array) {
        return toIntArray(array, true);
    }

    public Iterator<Integer> iterator() {
        return new BaseHashIterator(true);
    }
}
