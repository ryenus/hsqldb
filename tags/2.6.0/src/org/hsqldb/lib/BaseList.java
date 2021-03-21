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

/**
 * Abstract base for Lists
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.7.0
 */
abstract class BaseList<E> {

    protected ObjectComparator comparator = ObjectComparator.defaultComparator;

    protected int elementCount;

    public abstract E get(int index);

    public abstract E remove(int index);

    public abstract boolean add(E element);

    public abstract int size();

    public boolean contains(Object element) {

        for (int i = 0, size = size(); i < size; i++) {
            Object current = get(i);

            if (comparator.equals(current, element)) {
                return true;
            }
        }
        return false;
    }

    public boolean remove(Object element) {

        for (int i = 0, size = size(); i < size; i++) {
            Object current = get(i);

            if (comparator.equals(current, element)) {
                remove(i);

                return true;
            }
        }

        return false;
    }

    public int indexOf(E element) {

        for (int i = 0, size = size(); i < size; i++) {
            Object current = get(i);

            if (comparator.equals(current, element)) {
                return i;
            }
        }

        return -1;
    }

    public boolean addAll(Collection<? extends E> other) {

        boolean               result = false;
        Iterator<? extends E> it     = other.iterator();

        while (it.hasNext()) {
            result = true;

            add(it.next());
        }

        return result;
    }

    public boolean addAll(E[] array) {

        boolean  result = false;
        for ( E object : array ) {
          result = true;

          add(object);
        }

        return result;
    }

    public boolean isEmpty() {
        return elementCount == 0;
    }

    public String toString() {

        StringBuilder sb = new StringBuilder(32 + elementCount * 3);

        sb.append("List : size=");
        sb.append(elementCount);
        sb.append(' ');
        sb.append('{');

        Iterator<?> it = iterator();

        while (it.hasNext()) {
            sb.append(it.next());

            if (it.hasNext()) {
                sb.append(',');
                sb.append(' ');
            }
        }

        sb.append('}');

        return sb.toString();
    }

    public Iterator<E> iterator() {
        return new BaseListIterator();
    }

    private class BaseListIterator implements Iterator<E> {

        int     counter = 0;
        boolean removed;

        public boolean hasNext() {
            return counter < elementCount;
        }

        public E next() {

            if (counter < elementCount) {
                removed = false;

                E returnValue = get(counter);

                counter++;

                return returnValue;
            }

            throw new NoSuchElementException();
        }

        public int nextInt() {
            throw new NoSuchElementException();
        }

        public long nextLong() {
            throw new NoSuchElementException();
        }

        public void remove() {

            if (removed) {
                throw new NoSuchElementException("Iterator");
            }

            removed = true;

            if (counter != 0) {
                BaseList.this.remove(counter - 1);

                counter--;    // above can throw, so decrement if successful

                return;
            }

            throw new NoSuchElementException();
        }
    }
}
