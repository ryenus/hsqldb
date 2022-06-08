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

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Maintains an ordered  {@code long->long} lookup table, consisting of two
 * columns, one for keys, the other for values. Equal keys are allowed.<p>
 *
 * The table is sorted on key column.<p>
 *
 * findXXX() methods return the array index into the list
 * pair containing a matching key or value, or  or -1 if not found.<p>
 *
 * Based on org.hsqldb.lib.DoubleIntIndex
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.0
 * @since 1.8.0
 */
public final class DoubleLongIndex implements LongLookup {

    private int     count = 0;
    private int     capacity;
    private boolean sorted = true;
    private long[]  keys;
    private long[]  values;

//
    private long targetSearchValue;

    public DoubleLongIndex(int capacity) {

        this.capacity = capacity;
        keys          = new long[capacity];
        values        = new long[capacity];
    }

    public long getLongKey(int i) {

        if (i < 0 || i >= count) {
            throw new IndexOutOfBoundsException();
        }

        return keys[i];
    }

    public long getLongValue(int i) {

        if (i < 0 || i >= count) {
            throw new IndexOutOfBoundsException();
        }

        return values[i];
    }

    /**
     * Modifies an existing pair.
     * @param i the index
     * @param value the value
     */
    public void setLongValue(int i, long value) {

        if (i < 0 || i >= count) {
            throw new IndexOutOfBoundsException();
        }

        values[i] = value;
    }

    public int size() {
        return count;
    }

    public long getTotalValues() {

        long total = 0;

        for (int i = 0; i < count; i++) {
            total += values[i];
        }

        return total;
    }

    public void setSize(int newSize) {
        count = newSize;
    }

    public boolean addUnsorted(long key, long value) {

        if (count == capacity) {
            doubleCapacity();
        }

        if (sorted && count != 0) {
            if (key < keys[count - 1]) {
                sorted = false;
            }
        }

        keys[count]   = key;
        values[count] = value;

        count++;

        return true;
    }

    public int add(long key, long value) {

        if (count == capacity) {
            doubleCapacity();
        }

        if (!sorted) {
            fastQuickSort();
        }

        targetSearchValue = key;

        int i = binarySlotSearch(true);

        if (count != i) {
            moveRows(i, i + 1, count - i);
        }

        keys[i]   = key;
        values[i] = value;

        count++;

        return i;
    }

    public long lookup(long key) throws NoSuchElementException {

        int i = findFirstEqualKeyIndex(key);

        if (i == -1) {
            throw new NoSuchElementException();
        }

        return getLongValue(i);
    }

    public long lookup(long key, long def) {

        int i = findFirstEqualKeyIndex(key);

        if (i == -1) {
            return def;
        }

        return getLongValue(i);
    }

    public void clear() {

        Arrays.fill(keys, 0);
        Arrays.fill(values, 0);

        count  = 0;
        sorted = true;
    }

    public LongLookup duplicate() {

        DoubleLongIndex duplicate = new DoubleLongIndex(capacity);

        copyTo(duplicate);

        return duplicate;
    }

    /**
     * @param value the value
     * @return the index
     */
    public int findFirstGreaterEqualKeyIndex(long value) {

        int index = findFirstGreaterEqualSlotIndex(value);

        return index == count ? -1
                              : index;
    }

    /**
     * @param value the value
     * @return the index
     */
    public int findFirstEqualKeyIndex(long value) {

        if (!sorted) {
            fastQuickSort();
        }

        targetSearchValue = value;

        return binaryFirstSearch();
    }

    /**
     * This method is similar to findFirstGreaterEqualKeyIndex(int) but
     * returns the index of the empty row past the end of the array if
     * the search value is larger than all the values / keys in the searched
     * column.
     * @param value the value
     * @return the index
     */
    public int findFirstGreaterEqualSlotIndex(long value) {

        if (!sorted) {
            fastQuickSort();
        }

        targetSearchValue = value;

        return binarySlotSearch(false);
    }

    public boolean compactLookupAsIntervals() {

        if (size() == 0) {
            return false;
        }

        if (!sorted) {
            fastQuickSort();
        }

        int base = 0;

        for (int i = 1; i < count; i++) {
            long limit = keys[base] + values[base];

            if (limit == keys[i]) {
                values[base] += values[i];    // base updated
            } else {
                base++;

                keys[base]   = keys[i];
                values[base] = values[i];
            }
        }

        for (int i = base + 1; i < count; i++) {
            keys[i]   = 0;
            values[i] = 0;
        }

        if (count != base + 1) {
            setSize(base + 1);

            return true;
        }

        return false;
    }

    /**
     * Returns the index of the lowest element == the given search target,
     * or -1
     * @return index or -1 if not found
     */
    private int binaryFirstSearch() {

        int low     = 0;
        int high    = count;
        int mid     = 0;
        int compare = 0;
        int found   = count;

        while (low < high) {
            mid     = (low + high) >>> 1;
            compare = compare(mid);

            if (compare < 0) {
                high = mid;
            } else if (compare > 0) {
                low = mid + 1;
            } else {
                high  = mid;
                found = mid;
            }
        }

        return found == count ? -1
                              : found;
    }

    /**
     * Returns the index of the lowest element {@code >=} the given search target, or
     * count
     *
     * @return the index or count
     * @param fullCompare ignored
     */
    private int binarySlotSearch(boolean fullCompare) {

        int low     = 0;
        int high    = count;
        int mid     = 0;
        int compare = 0;

        while (low < high) {
            mid     = (low + high) >>> 1;
            compare = compare(mid);

            if (compare <= 0) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        return low;
    }

    public void sort() {

        if (count <= 1024 * 16) {
            fastQuickSortRecursive();
        } else {
            fastQuickSort();
        }
    }

    /**
     * fast quicksort using a stack on the heap to reduce stack use
     */
    private void fastQuickSort() {

        DoubleIntIndex indices   = new DoubleIntIndex(32768);
        int            threshold = 16;

        indices.push(0, count - 1);

        while (indices.size() > 0) {
            int start = indices.peekKey();
            int end   = indices.peekValue();

            indices.pop();

            if (end - start >= threshold) {
                int pivot = partition(start, end);

                indices.push(start, pivot - 1);
                indices.push(pivot + 1, end);
            }
        }

        insertionSort(0, count - 1);

        sorted = true;
    }

    private int partition(int start, int end) {

        int pivot = (start + end) >>> 1;

        // pivot is median of three values
        if (keys[pivot] < keys[(start + pivot) >>> 1]) {
            swap(pivot, (start + pivot) >>> 1);
        }

        if (keys[(end + pivot) >>> 1] < keys[(start + pivot) >>> 1]) {
            swap((end + pivot) >>> 1, (start + pivot) >>> 1);
        }

        if (keys[(end + pivot) >>> 1] < keys[pivot]) {
            swap((end + pivot) >>> 1, pivot);
        }

        long pivotValue = keys[pivot];
        int  i          = start - 1;
        int  j          = end;

        swap(pivot, end);

        for (;;) {
            while (keys[++i] < pivotValue) {}

            while (pivotValue < keys[--j]) {}

            if (j < i) {
                break;
            }

            swap(i, j);
        }

        swap(i, end);

        return i;
    }

    /**
     * fast quicksort with recursive quicksort implementation
     */
    private void fastQuickSortRecursive() {

        quickSort(0, count - 1);
        insertionSort(0, count - 1);

        sorted = true;
    }

    private void quickSort(int l, int r) {

        int M = 16;
        int i;
        int j;
        int v;

        if ((r - l) > M) {
            i = (r + l) >>> 1;

            if (lessThan(i, l)) {
                swap(l, i);    // Tri-Median Method!
            }

            if (lessThan(r, l)) {
                swap(l, r);
            }

            if (lessThan(r, i)) {
                swap(i, r);
            }

            j = r - 1;

            swap(i, j);

            i = l;
            v = j;

            for (;;) {
                while (lessThan(++i, v)) {}

                while (lessThan(v, --j)) {}

                if (j < i) {
                    break;
                }

                swap(i, j);
            }

            swap(i, r - 1);
            quickSort(l, j);
            quickSort(i + 1, r);
        }
    }

    private void insertionSort(int lo0, int hi0) {

        int i;
        int j;

        for (i = lo0 + 1; i <= hi0; i++) {
            j = i;

            while ((j > lo0) && lessThan(i, j - 1)) {
                j--;
            }

            if (i != j) {
                moveAndInsertRow(i, j);
            }
        }
    }

    private void moveAndInsertRow(int i, int j) {

        long col1 = keys[i];
        long col2 = values[i];

        moveRows(j, j + 1, i - j);

        keys[j]   = col1;
        values[j] = col2;
    }

    private void swap(int i1, int i2) {

        long col1 = keys[i1];
        long col2 = values[i1];

        keys[i1]   = keys[i2];
        values[i1] = values[i2];
        keys[i2]   = col1;
        values[i2] = col2;
    }

    /**
     * Check if targeted column value in the row indexed i is less than the
     * search target object.
     * @param i the index
     * @return -1, 0 or +1
     */
    private int compare(int i) {

        if (targetSearchValue > keys[i]) {
            return 1;
        } else if (targetSearchValue < keys[i]) {
            return -1;
        }

        return 0;
    }

    /**
     * Check if row indexed i is less than row indexed j
     * @param i the first index
     * @param j the second index
     * @return true or false
     */
    private boolean lessThan(int i, int j) {

        if (keys[i] < keys[j]) {
            return true;
        }

        return false;
    }

    private void moveRows(int fromIndex, int toIndex, int rows) {
        System.arraycopy(keys, fromIndex, keys, toIndex, rows);
        System.arraycopy(values, fromIndex, values, toIndex, rows);
    }

    private void doubleCapacity() {

        keys     = (long[]) ArrayUtil.resizeArray(keys, capacity * 2);
        values   = (long[]) ArrayUtil.resizeArray(values, capacity * 2);
        capacity *= 2;
    }

    public void copyTo(DoubleLongIndex other) {

        System.arraycopy(keys, 0, other.keys, 0, count);
        System.arraycopy(values, 0, other.values, 0, count);
        other.setSize(count);
    }

    public boolean addUnsorted(LongLookup other) {

        if (!ensureCapacityToAdd(other.size())) {
            return false;
        }

        sorted = false;

        for (int i = 0; i < other.size(); i++) {
            long key   = other.getLongKey(i);
            long value = other.getLongValue(i);

            this.addUnsorted(key, value);
        }

        return true;
    }

    private boolean ensureCapacityToAdd(int extra) {

        if (count + extra > capacity) {
            while (count + extra > capacity) {
                doubleCapacity();
            }
        }

        return true;
    }
}
