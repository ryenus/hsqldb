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
import java.util.Comparator;

// fredt@users - 1.8.0 - 2.5.x - enhancements

/**
 * Intended as an asynchronous alternative to Vector.
 *
 * @author dnordahl@users
 * @version 2.6.0
 * @since 1.7.0
 */
public class HsqlArrayList<E> extends BaseList<E> implements List<E> {

//fredt@users
/*
    private static Reporter reporter = new Reporter();

    private static class Reporter {

        private static int initCounter   = 0;
        private static int updateCounter = 0;

        Reporter() {

            try {
                System.runFinalizersOnExit(true);
            } catch (SecurityException e) {}
        }

        protected void finalize() {

            System.out.println("HsqlArrayList init count: " + initCounter);
            System.out.println("HsqlArrayList update count: "
                               + updateCounter);
        }
    }
*/
    private static final int   DEFAULT_INITIAL_CAPACITY = 8;
    private static final float DEFAULT_RESIZE_FACTOR    = 2.0f;
    E[]                        elementData;
    E[]                        reserveElementData;
    private boolean            minimizeOnClear;

    public HsqlArrayList(E[] data, int count) {
        elementData  = data;
        elementCount = count;
    }

    /** Creates a new instance of HsqlArrayList */
    @SuppressWarnings( "unchecked" )
    public HsqlArrayList() {

//        reporter.initCounter++;
        elementData = (E[]) new Object[DEFAULT_INITIAL_CAPACITY];
    }

    /**
     * Creates a new instance of HsqlArrayList that minimizes the size when empty
     *
     * @param initialCapacity int
     * @param minimize boolean
     */
    @SuppressWarnings( "unchecked" )
    public HsqlArrayList(int initialCapacity, boolean minimize) {

//        reporter.initCounter++;
        if (initialCapacity < DEFAULT_INITIAL_CAPACITY) {
            initialCapacity = DEFAULT_INITIAL_CAPACITY;
        }

        elementData     = (E[]) new Object[initialCapacity];
        minimizeOnClear = minimize;
    }

    /**
     * Creates a new instance with the given initial capacity
     *
     * @param initialCapacity int
     */
    @SuppressWarnings( "unchecked" )
    public HsqlArrayList(int initialCapacity) {

//        reporter.initCounter++;
        if (initialCapacity < 0) {
            throw new NegativeArraySizeException(
                "Invalid initial capacity given");
        }

        if (initialCapacity < DEFAULT_INITIAL_CAPACITY) {
            initialCapacity = DEFAULT_INITIAL_CAPACITY;
        }

        elementData = (E[]) new Object[initialCapacity];
    }

    /**
     * Inserts an element at the given index
     *
     * @param index int
     * @param element E
     */
    public void add(int index, E element) {

//        reporter.updateCounter++;
        if (index > elementCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + ">" + elementCount);
        }

        if (index < 0) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " < 0");
        }

        if (elementCount >= elementData.length) {
            increaseCapacity();
        }

        if (index < elementCount) {
            System.arraycopy(elementData, index, elementData, index + 1,
                             elementCount - index);
        }

        elementData[index] = element;

        elementCount++;
    }

    /**
     * Appends an element to the end of the list
     *
     * @param element E
     * @return boolean
     */
    @Override
    public boolean add(E element) {

//        reporter.updateCounter++;
        if (elementCount >= elementData.length) {
            increaseCapacity();
        }

        elementData[elementCount] = element;

        elementCount++;

        return true;
    }

    /**
     * Gets the element at given position
     *
     * @param index int
     * @return E
     */
    @Override
    public E get(int index) {

        if (index >= elementCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " >= "
                                                + elementCount);
        }

        return elementData[index];
    }

    /**
     * returns the index of given object or -1 if not found
     *
     * @param o Object
     * @return int
     */
    @Override
    public int indexOf(Object o) {

        if (o == null) {
            for (int i = 0; i < elementCount; i++) {
                if (elementData[i] == null) {
                    return i;
                }
            }

            return -1;
        }

        for (int i = 0; i < elementCount; i++) {
            if (comparator.equals(o, elementData[i])) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Removes and returns the element at given position
     *
     * @param index int
     * @return E
     */
    @Override
    public E remove(int index) {

        if (index >= elementCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " >= "
                                                + elementCount);
        }

        E removedObj = elementData[index];

        if (index < elementCount - 1) {
            System.arraycopy(elementData, index + 1, elementData, index,
                             elementCount - 1 - index);
        }

        elementCount--;

        elementData[elementCount] = null;

        if (elementCount == 0) {
            clear();
        }

        return removedObj;
    }

    /**
     * Replaces the element at given position
     *
     * @param index int
     * @param element E
     * @return E
     */
    public E set(int index, E element) {

        if (index >= elementCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " >= "
                                                + elementCount);
        }

        E replacedObj = elementData[index];

        elementData[index] = element;

        return replacedObj;
    }

    /**
     * Returns the number of elements in the array list
     *
     * @return int
     */
    @Override
    public final int size() {
        return elementCount;
    }

    private void increaseCapacity() {

        int baseSize = elementData.length == 0 ? 1
                                               : elementData.length;

        baseSize = (int) (baseSize * DEFAULT_RESIZE_FACTOR);

        resize(baseSize);
    }

    private void resize(int baseSize) {

        if (baseSize == elementData.length) {
            return;
        }

        @SuppressWarnings( "unchecked" )
        E[] newArray = (E[]) Array.newInstance(
            elementData.getClass().getComponentType(), baseSize);
        int count = elementData.length > newArray.length ? newArray.length
                                                         : elementData.length;

        System.arraycopy(elementData, 0, newArray, 0, count);

        if (minimizeOnClear && reserveElementData == null) {
            Arrays.fill(elementData, 0);

            reserveElementData = elementData;
        }

        elementData = newArray;
    }

    /** Trims the array to be the same size as the number of elements. */
    public void trim() {

        // 0 size array is possible
        resize(elementCount);
    }

    public void clear() {

        if (minimizeOnClear && reserveElementData != null) {
            elementData        = reserveElementData;
            reserveElementData = null;
            elementCount       = 0;

            return;
        }

        for (int i = 0; i < elementCount; i++) {
            elementData[i] = null;
        }

        elementCount = 0;
    }

    /**
     * Increase or reduce the size, setting discarded or added elements to null.
     *
     * @param newSize int
     */
    public void setSize(int newSize) {

        if (newSize == 0) {
            clear();

            return;
        }

        if (newSize <= elementCount) {
            for (int i = newSize; i < elementCount; i++) {
                elementData[i] = null;
            }

            elementCount = newSize;

            return;
        }

        for (; newSize > elementData.length; ) {
            increaseCapacity();
        }

        elementCount = newSize;
    }

    public Object[] toArray() {

        Object[] newArray = new Object[elementCount];

        System.arraycopy(elementData, 0, newArray, 0, elementCount);

        return newArray;
    }

    /**
     * Copies all elements of the list to a[].<p>
     *
     * If a[] is too small, a new array or the same type is returned.<p>
     *
     * If a[] is larger, only the list elements are copied and no other
     * change is made to the array.<p>
     *
     * Differs from the implementation in java.util.ArrayList in the second
     * aspect.
     *
     * @param <T> type of array element
     * @param array T[]
     * @return T[]
     */
    public <T> T[] toArray(T[] array) {

        if (array.length < elementCount) {
            array = (T[]) Array.newInstance(array.getClass().getComponentType(),
                                        elementCount);
        }

        System.arraycopy(elementData, 0, array, 0, elementCount);

        return array;
    }

    /**
     * Copies elements of the list from start to limit to array. The array must
     * be large enough.
     *
     * @param array E[]
     * @param start int
     * @param limit int
     */
    public void toArraySlice(E[] array, int start, int limit) {
        System.arraycopy(elementData, start, array, 0, limit - start);
    }

    public E[] getArray() {
        return elementData;
    }

    public void sort(Comparator<? super E> c) {

        if (elementCount < 2) {
            return;
        }

        ArraySort.sort(elementData, elementCount, c);
    }

    public int lastIndexOf(Object o) {

        if (o == null) {
            for (int i = elementCount - 1; i >= 0; i--) {
                if (elementData[i] == null) {
                    return i;
                }
            }

            return -1;
        }

        for (int i = elementCount - 1; i >= 0; i--) {
            if (comparator.equals(o, elementData[i])) {
                return i;
            }
        }

        return -1;
    }
}
