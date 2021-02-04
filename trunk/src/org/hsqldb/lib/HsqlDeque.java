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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * A {@code List<E>} that also implements {@code Deque<E>} and {@code Queue<E>}
 * and methods for usage as stack.<p>
 *
 * When used as {@code Queue<E>}, elements are added to the end of
 * the List (tail), and retrieved from the start of the List (head).<p>
 *
 * When used as a stack, elements are added to and retrieved from the start of
 * the List (head) using {@code push()} and {@code pop()} methods.<p>
 *
 * Data is stored in an Object[] that doubles in size when the List gets full
 * but does not shrink when it gets empty.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.7.0, 20020130
 */
public class HsqlDeque<E> extends BaseList<E> implements List<E> {

    private E[] list;
    private int firstindex = 0;    // index of first list element
    private int endindex   = 0;    // index of last list element + 1

    // can grow to fill list
    // if elementCount == 0 then firstindex == endindex
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    public HsqlDeque() {
        list = (E[]) new Object[DEFAULT_INITIAL_CAPACITY];
    }

    public int size() {
        return elementCount;
    }

    public boolean isEmpty() {
        return elementCount == 0;
    }

    public boolean offer(E e) {
        return add(e); }

    public E remove() {
        return removeFirst();
    }

    public E poll() {
        if(elementCount == 0) {
            return null;
        }

        return removeFirst();
    }

    public E element() {
        return getFirst();
    }

    public E peek() {
        if(elementCount == 0) {
            return null;
        }

        return getFirst();
    }

    public void push(E e) {
        addFirst(e);
    }

    public E pop() {
        return removeFirst();
    }

    public E getFirst() throws NoSuchElementException {

        if (elementCount == 0) {
            throw new NoSuchElementException();
        }

        return list[firstindex];
    }

    public E getLast() throws NoSuchElementException {

        if (elementCount == 0) {
            throw new NoSuchElementException();
        }

        return list[endindex - 1];
    }

    public E get(int i) throws IndexOutOfBoundsException {

        int index = getInternalIndex(i);

        return list[index];
    }

    public void add(int i, E o) throws IndexOutOfBoundsException {

        if (i == elementCount) {
            add(o);

            return;
        }

        resetCapacity();

        int index = getInternalIndex(i);

        if (index < endindex && endindex < list.length) {
            System.arraycopy(list, index, list, index + 1, endindex - index);

            endindex++;
        } else {
            System.arraycopy(list, firstindex, list, firstindex - 1,
                             index - firstindex);

            firstindex--;
            index--;
        }

        list[index] = o;

        elementCount++;
    }

    public E set(int i, E o) throws IndexOutOfBoundsException {

        int index  = getInternalIndex(i);
        E   result = list[index];

        list[index] = o;

        return result;
    }

    public E removeFirst() throws NoSuchElementException {

        if (elementCount == 0) {
            throw new NoSuchElementException();
        }

        E o = list[firstindex];

        list[firstindex] = null;

        firstindex++;
        elementCount--;

        if (elementCount == 0) {
            firstindex = endindex = 0;
        } else if (firstindex == list.length) {
            firstindex = 0;
        }

        return o;
    }

    public E removeLast() throws NoSuchElementException {

        if (elementCount == 0) {
            throw new NoSuchElementException();
        }

        endindex--;

        E o = list[endindex];

        list[endindex] = null;

        elementCount--;

        if (elementCount == 0) {
            firstindex = endindex = 0;
        } else if (endindex == 0) {
            endindex = list.length;
        }

        return o;
    }

    public E peekFirst() {
        return getFirst();
    }

    public E peekLast() {
        return getLast();
    }

    public boolean offerFirst(E e) {
        addFirst(e);
        return true;
    }

    public boolean offerLast(E e) {
        addLast(e);
        return true;
    }

    public E pollFirst() {
        if (elementCount == 0) {
            return null;
        }

        return removeFirst();
    }

    public E pollLast() {
        if (elementCount == 0) {
            return null;
        }

        return removeLast();
    }

    public boolean removeFirstOccurrence(Object o) {
        int index = indexOf(o);

        if (index < 1) {
            return false;
        }

        remove(index);

        return true;
    }

    public boolean removeLastOccurrence(Object o) {
        int index = lastIndexOf(o);

        if (index < 1) {
            return false;
        }

        remove(index);

        return true;
    }

    public Iterator<E> descendingIterator() {
        throw new UnsupportedOperationException();
    }

    public boolean add(E o) {

        resetCapacity();

        if (endindex == list.length) {
            endindex = 0;
        }

        list[endindex] = o;

        elementCount++;
        endindex++;

        return true;
    }

    public void addLast(E o) {
        add(o);
    }

    public void addFirst(E o) {

        resetCapacity();

        firstindex--;

        if (firstindex < 0) {
            firstindex = list.length - 1;

            if (endindex == 0) {
                endindex = list.length;
            }
        }

        list[firstindex] = o;

        elementCount++;
    }

    public void clear() {

        if (elementCount == 0) {
            return;
        }

        firstindex = endindex = elementCount = 0;

        Arrays.fill(list, null);
    }

    public int indexOf(Object value) {

        for (int i = 0; i < elementCount; i++) {
            int index = firstindex + i;

            if (index >= list.length) {
                index -= list.length;
            }

            if (list[index] == value) {
                return i;
            }

            if (value != null && comparator.equals(value, list[index])) {
                return i;
            }
        }

        return -1;
    }

    public int lastIndexOf(Object value) {

        for (int i = elementCount - 1; i >= 0; i--) {
            int index = firstindex + i;

            if (index < 0) {
                index += list.length;
            }

            if (list[index] == value) {
                return i;
            }

            if (value != null && comparator.equals(value, list[index])) {
                return i;
            }
        }

        return -1;
    }


    public E remove(int index) {

        int target = getInternalIndex(index);
        E   value  = list[target];

        if (target == firstindex) {
            list[firstindex] = null;

            firstindex++;

            if (firstindex == list.length) {
                firstindex = 0;
            }
        } else if (target > firstindex) {
            System.arraycopy(list, firstindex, list, firstindex + 1,
                             target - firstindex);

            list[firstindex] = null;

            firstindex++;

            if (firstindex == list.length) {
                firstindex = 0;
            }
        } else {
            System.arraycopy(list, target + 1, list, target,
                             endindex - target - 1);

            endindex--;

            list[endindex] = null;

            if (endindex == 0) {
                endindex = list.length;
            }
        }

        elementCount--;

        if (elementCount == 0) {
            firstindex = endindex = 0;
        }

        return value;
    }

    private int getInternalIndex(int i) throws IndexOutOfBoundsException {

        if (i < 0 || i >= elementCount) {
            throw new IndexOutOfBoundsException();
        }

        int index = firstindex + i;

        if (index >= list.length) {
            index -= list.length;
        }

        return index;
    }

    private void resetCapacity() {

        if (elementCount < list.length) {
            return;
        }

        Object[] newList = new Object[list.length * 2];

        System.arraycopy(list, firstindex, newList, firstindex,
                         list.length - firstindex);

        if (endindex <= firstindex) {
            System.arraycopy(list, 0, newList, list.length, endindex);

            endindex = list.length + endindex;
        }

        list = (E[]) newList;
    }

    public <T> T[] toArray(T[] array) {

        if (array.length < elementCount) {
            array = (T[]) Array.newInstance(array.getClass().getComponentType(),
                                        elementCount);
        }

        int tempCount = list.length - firstindex;

        if (tempCount > elementCount) {
            tempCount = elementCount;
        }

        System.arraycopy(list, firstindex, array, 0, tempCount);

        if (endindex <= firstindex) {
            System.arraycopy(list, 0, array, tempCount, endindex);
        }

        return array;
    }

    public Object[] toArray() {

        Object[] newArray = new Object[elementCount];

        return toArray(newArray);
    }

    public List<E> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    public boolean containsAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(int index, Collection c) {
        throw new UnsupportedOperationException();
    }
}
