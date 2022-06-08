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


package org.hsqldb.map;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.hsqldb.lib.ArrayCounter;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.ObjectComparator;
import org.hsqldb.lib.PrimitiveIterator;

/**
 * Base class for hash tables or sets. The exact type of the structure is
 * defined by the constructor. Each instance has at least a keyTable array
 * and a HashIndex instance for looking up the keys into this table. Instances
 * that are maps also have a valueTable the same size as the keyTable.
 *
 * Special getOrAddXXX() methods are used for object maps in some subclasses.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.7.2
 */
public class BaseHashMap {

/*

    data store:
    keys: {array of primitive | array of object}
    values: {none | array of primitive | array of object} same size as keys
    objects support : hashCode(), equals()

    implemented types of keyTable:
    {objectKeyTable: variable size Object[] array for keys |
    intKeyTable: variable size int[] for keys |
    longKeyTable: variable size long[] for keys }

    implemented types of valueTable:
    {objectValueTable: variable size Object[] array for values |
    intValueTable: variable size int[] for values |
    longValueTable: variable size long[] for values}

    valueTable does not exist for sets or for object pools

    hash index:
    hashTable: fixed size int[] array for hash lookup into keyTable
    linkTable: pointer to the next key ; size equal or larger than hashTable
    but equal to the valueTable

    access count table:
    {none |
    variable size int[] array for access count} same size as xxxKeyTable
*/

    //
    protected boolean isIntKey;
    protected boolean isLongKey;
    protected boolean isObjectKey;
    protected boolean isNoValue;
    protected boolean isIntValue;
    protected boolean isLongValue;
    protected boolean isObjectValue;
    protected boolean isMultiValue;
    protected boolean isTwoObjectValue;
    protected boolean isList;
    protected boolean isAccessCount;
    protected boolean isLastAccessCount;

    //
    protected HashIndex hashIndex;

    //
    protected int[]    intKeyTable;
    protected Object[] objectKeyTable;
    protected long[]   longKeyTable;

    //
    protected int[]    intValueTable;
    protected Object[] objectValueTable;
    protected long[]   longValueTable;

    //
    protected int           accessMin;
    protected AtomicInteger accessCount;
    protected int[]         accessTable;
    protected Object[]      objectValueTable2;

    //
    protected final float      loadFactor;
    protected final int        initialCapacity;
    protected int              threshold;
    protected int              maxCapacity;
    protected int              purgePolicy = NO_PURGE;
    protected boolean          minimizeOnEmpty;
    protected ObjectComparator comparator = ObjectComparator.defaultComparator;

    //
    protected boolean hasZeroKey;
    protected int     zeroKeyIndex = -1;
    protected int     zeroOrNullValueCount;

    // keyOrValueTypes
    protected static final int noKeyOrValue     = 0;
    protected static final int intKeyOrValue    = 1;
    protected static final int longKeyOrValue   = 2;
    protected static final int objectKeyOrValue = 3;

    // purgePolicy
    protected static final int NO_PURGE   = 0;
    protected static final int PURGE_ALL  = 1;
    protected static final int PURGE_HALF = 2;

    //
    public static final int      ACCESS_MAX = Integer.MAX_VALUE - (1 << 20);
    public static final Object[] emptyObjectArray = new Object[]{};

    protected BaseHashMap(int initialCapacity, int keyType, int valueType,
                          boolean hasAccessCount)
                          throws IllegalArgumentException {

        if (initialCapacity <= 0) {
            throw new IllegalArgumentException();
        }

        if (initialCapacity < 4) {
            initialCapacity = 4;
        } else {
            initialCapacity = (int) ArrayUtil.getBinaryNormalisedCeiling(initialCapacity);
        }

        this.loadFactor      = 1;    // can use any value if necessary
        this.initialCapacity = initialCapacity;
        threshold            = initialCapacity;

        int hashtablesize = (int) (initialCapacity * loadFactor);

        if (hashtablesize < 4) {
            hashtablesize = 4;
        }

        hashIndex = new HashIndex(hashtablesize, initialCapacity, true);

        int arraySize = threshold;

        if (keyType == BaseHashMap.intKeyOrValue) {
            isIntKey    = true;
            intKeyTable = new int[arraySize];
        } else if (keyType == BaseHashMap.objectKeyOrValue) {
            isObjectKey    = true;
            objectKeyTable = new Object[arraySize];
        } else {
            isLongKey    = true;
            longKeyTable = new long[arraySize];
        }

        if (valueType == BaseHashMap.intKeyOrValue) {
            isIntValue    = true;
            intValueTable = new int[arraySize];
        } else if (valueType == BaseHashMap.objectKeyOrValue) {
            isObjectValue    = true;
            objectValueTable = new Object[arraySize];
        } else if (valueType == BaseHashMap.longKeyOrValue) {
            isLongValue    = true;
            longValueTable = new long[arraySize];
        } else {
            isNoValue = true;
        }

        isLastAccessCount = hasAccessCount;

        if (hasAccessCount) {
            accessTable = new int[arraySize];
            accessCount = new AtomicInteger();
        }
    }

    protected int getLookup(Object key) {

        int hash   = comparator.hashCode(key);
        int lookup = hashIndex.getLookup(hash);

        for (; lookup >= 0; lookup = hashIndex.getNextLookup(lookup)) {
            Object current = objectKeyTable[lookup];

            if (comparator.equals(key, current)) {
                break;
            }
        }

        return lookup;
    }

    protected int getLookup(Object key, int hash) {

        int lookup = hashIndex.getLookup(hash);

        for (; lookup >= 0; lookup = hashIndex.getNextLookup(lookup)) {
            Object current = objectKeyTable[lookup];

            if (comparator.equals(key, current)) {
                break;
            }
        }

        return lookup;
    }

    protected int getLookup(int key) {

        int hash   = (int) ((long) key >>> 32 ^ key);
        int lookup = hashIndex.getLookup(hash);

        for (; lookup >= 0; lookup = hashIndex.getNextLookup(lookup)) {
            int current = intKeyTable[lookup];

            if (key == current) {
                break;
            }
        }

        return lookup;
    }

    protected int getLookup(long key) {

        int hash   = (int) (key >>> 32 ^ key);
        int lookup = hashIndex.getLookup(hash);

        for (; lookup >= 0; lookup = hashIndex.getNextLookup(lookup)) {
            long current = longKeyTable[lookup];

            if (key == current) {
                break;
            }
        }

        return lookup;
    }

    protected int getObjectLookup(long key) {

        int  hash   = (int) (key >>> 32 ^ key);
        int  lookup = hashIndex.getLookup(hash);
        long tempKey;

        for (; lookup >= 0; lookup = hashIndex.getNextLookup(lookup)) {
            tempKey = comparator.longKey(objectKeyTable[lookup]);

            if (tempKey == key) {
                break;
            }
        }

        return lookup;
    }

    protected PrimitiveIterator getMultiValuesIterator(Object key) {

        int lookup = getLookup(key);
        ValueCollectionIterator valuesIterator =
            new ValueCollectionIterator(key, lookup);

        return valuesIterator;
    }

    protected int multiValueElementCount(Object key) {

        int lookup = getLookup(key);

        if (lookup == -1) {
            return 0;
        }

        int count = 1;

        while (true) {
            lookup = BaseHashMap.this.hashIndex.getNextLookup(lookup);

            if (lookup == -1) {
                break;
            }

            if (BaseHashMap.this.objectKeyTable[lookup].equals(key)) {
                count++;
            } else {
                break;
            }
        }

        return count;
    }

    protected int multiValueKeyCount() {

        int    count  = 0;
        int    lookup = -1;
        Object oldKey = null;

        for (int index = 0; index < hashIndex.hashTable.length; ) {
            if (hashIndex.hashTable[index] < 0) {
                index++;

                continue;
            }

            if (lookup < 0) {
                lookup = hashIndex.hashTable[index];
            } else {
                lookup = hashIndex.getNextLookup(lookup);
            }

            if (lookup < 0) {
                index++;

                continue;
            }

            if (!comparator.equals(oldKey, objectKeyTable[lookup])) {
                oldKey = objectKeyTable[lookup];

                count++;
            }
        }

        return count;
    }

    /**
     * generic method for adding keys and values or updating values
     *
     * returns existing Object value if any or null
     *
     * returns
     */
    protected Object addOrUpdate(long longKey, long longValue,
                                 Object objectKey, Object objectValue) {

        int hash;

        if (isObjectKey) {
            if (objectKey == null) {
                return null;
            }

            hash = comparator.hashCode(objectKey);
        } else {
            hash = (int) (longKey >>> 32 ^ longKey);
        }

        int     index       = hashIndex.getHashIndex(hash);
        int     lookup      = hashIndex.hashTable[index];
        int     lastLookup  = -1;
        Object  returnValue = null;
        boolean matched     = false;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            if (isObjectKey) {
                matched = comparator.equals(objectKeyTable[lookup], objectKey);
            } else if (isIntKey) {
                matched = longKey == intKeyTable[lookup];
            } else if (isLongKey) {
                matched = longKey == longKeyTable[lookup];
            }

            if (matched) {
                break;
            }
        }

        if (matched) {
            if (isNoValue) {
                return Boolean.FALSE;
            } else if (isObjectValue) {
                returnValue              = objectValueTable[lookup];
                objectValueTable[lookup] = objectValue;

                if (objectValue == null) {
                    if (returnValue != null) {
                        zeroOrNullValueCount++;
                    }
                } else {
                    if (returnValue == null) {
                        zeroOrNullValueCount--;
                    }
                }
            } else if (isIntValue) {
                int existing = intValueTable[lookup];

                returnValue           = existing;
                intValueTable[lookup] = (int) longValue;

                if (longValue == 0) {
                    if (existing != 0) {
                        zeroOrNullValueCount++;
                    }
                } else {
                    if (existing == 0) {
                        zeroOrNullValueCount--;
                    }
                }
            } else if (isLongValue) {
                long existing = longValueTable[lookup];

                returnValue            = existing;
                longValueTable[lookup] = longValue;

                if (longValue == 0) {
                    if (existing != 0) {
                        zeroOrNullValueCount++;
                    }
                } else {
                    if (existing == 0) {
                        zeroOrNullValueCount--;
                    }
                }
            }

            if (isLastAccessCount) {
                accessTable[lookup] = accessCount.incrementAndGet();
            } else if (isAccessCount) {
                accessTable[lookup]++;
            }

            return returnValue;
        }

        if (hashIndex.elementCount >= threshold) {
            if (reset()) {
                return addOrUpdate(longKey, longValue, objectKey, objectValue);
            } else {
                throw new NoSuchElementException("BaseHashMap");
            }
        }

        lookup = hashIndex.linkNode(index, lastLookup);

        if (isObjectKey) {
            objectKeyTable[lookup] = objectKey;
        } else if (isIntKey) {
            intKeyTable[lookup] = (int) longKey;

            if (longKey == 0) {
                hasZeroKey   = true;
                zeroKeyIndex = lookup;
            }
        } else if (isLongKey) {
            longKeyTable[lookup] = longKey;

            if (longKey == 0) {
                hasZeroKey   = true;
                zeroKeyIndex = lookup;
            }
        }

        if (isNoValue) {
            return Boolean.TRUE;
        } else if (isObjectValue) {
            objectValueTable[lookup] = objectValue;

            if (objectValue == null) {
                zeroOrNullValueCount++;
            }
        } else if (isIntValue) {
            intValueTable[lookup] = (int) longValue;

            if (longValue == 0) {
                zeroOrNullValueCount++;
            }
        } else if (isLongValue) {
            longValueTable[lookup] = longValue;

            if (longValue == 0) {
                zeroOrNullValueCount++;
            }
        }

        //
        if (isLastAccessCount) {
            accessTable[lookup] = accessCount.incrementAndGet();
        } else if (isAccessCount) {
            accessTable[lookup] = 1;
        }

        return returnValue;
    }

    /**
     * generic method for removing keys
     *
     * returns existing Object value if any (or Object key if this is a set)
     */
    protected Object remove(long longKey, long longValue, Object objectKey,
                            Object objectValue, boolean matchValue,
                            boolean removeRow) {

        int hash;

        if (isObjectKey) {
            if (objectKey == null) {
                return null;
            }

            hash = comparator.hashCode(objectKey);
        } else {
            hash = (int) (longKey >>> 32 ^ longKey);
        }

        int    index       = hashIndex.getHashIndex(hash);
        int    lookup      = hashIndex.hashTable[index];
        int    lastLookup  = -1;
        Object returnValue = null;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            boolean matched = false;

            if (isObjectKey) {
                matched = comparator.equals(objectKeyTable[lookup], objectKey);
            } else if (isIntKey) {
                matched = longKey == intKeyTable[lookup];
            } else if (isLongKey) {
                matched = longKey == longKeyTable[lookup];
            }

            if (matched) {
                if (matchValue) {
                    if (isObjectValue) {
                        matched = ObjectComparator.defaultComparator.equals(
                            objectValueTable[lookup], objectValue);
                    } else if (isIntValue) {
                        matched = intValueTable[lookup] == longValue;
                    } else if (isLongKey) {
                        matched = longValueTable[lookup] == longValue;
                    }

                    if (!matched) {
                        return null;
                    }
                }

                break;
            }
        }

        if (lookup < 0) {
            if (isNoValue) {
                return Boolean.FALSE;
            }

            return null;
        }

        if (isObjectKey) {
            objectKeyTable[lookup] = null;
        } else {
            if (longKey == 0) {
                hasZeroKey   = false;
                zeroKeyIndex = -1;
            }

            if (isIntKey) {
                intKeyTable[lookup] = 0;
            } else {
                longKeyTable[lookup] = 0;
            }
        }

        if (isNoValue) {
            returnValue = Boolean.TRUE;
        } else if (isObjectValue) {
            returnValue              = objectValueTable[lookup];
            objectValueTable[lookup] = null;

            if (returnValue == null) {
                zeroOrNullValueCount--;
            }
        } else if (isIntValue) {
            int existing = intValueTable[lookup];

            returnValue           = existing;
            intValueTable[lookup] = 0;

            if (existing == 0) {
                zeroOrNullValueCount--;
            }
        } else if (isLongValue) {
            long existing = longValueTable[lookup];

            returnValue            = existing;
            longValueTable[lookup] = 0;

            if (existing == 0) {
                zeroOrNullValueCount--;
            }
        }

        hashIndex.unlinkNode(index, lastLookup, lookup);

        if (accessTable != null) {
            accessTable[lookup] = 0;
        }

        if (isList && removeRow) {
            removeRow(lookup);
        }

        if (minimizeOnEmpty && hashIndex.elementCount == 0) {
            rehash(initialCapacity);
        }

        return returnValue;
    }

    /**
     * Single method for adding key / values in multi-value maps.
     * Values for each key are clustered.
     */
    protected boolean addMultiVal(long longKey, long longValue,
                                  Object objectKey, Object objectValue) {

        int hash;

        if (isObjectKey) {
            if (objectKey == null) {
                return false;
            }

            hash = comparator.hashCode(objectKey);
        } else {
            hash = (int) (longKey >>> 32 ^ longKey);
        }

        int     index      = hashIndex.getHashIndex(hash);
        int     lookup     = hashIndex.hashTable[index];
        int     lastLookup = -1;
        int     matchedKey = -1;
        boolean matched    = false;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            if (isObjectKey) {
                matched = comparator.equals(objectKeyTable[lookup], objectKey);
            } else if (isIntKey) {
                matched = longKey == intKeyTable[lookup];
            } else if (isLongKey) {
                matched = longKey == longKeyTable[lookup];
            }

            if (matched) {
                matchedKey = lookup;
            } else {
                if (matchedKey < 0) {
                    continue;
                } else {
                    break;
                }
            }

            if (isObjectValue) {
                matched = ObjectComparator.defaultComparator.equals(
                    objectValueTable[lookup], objectValue);
            } else if (isIntValue) {
                matched = longValue == intValueTable[lookup];
            } else if (isLongValue) {
                matched = longValue == longValueTable[lookup];
            }

            if (matched) {
                return false;
            }
        }

        if (hashIndex.elementCount >= threshold) {
            if (reset()) {
                return addMultiVal(longKey, longValue, objectKey, objectValue);
            } else {
                throw new NoSuchElementException("BaseHashMap");
            }
        }

        lookup = hashIndex.linkNode(index, lastLookup);

        // type dependent block
        if (isObjectKey) {
            objectKeyTable[lookup] = objectKey;
        } else if (isIntKey) {
            intKeyTable[lookup] = (int) longKey;

            if (longKey == 0) {
                hasZeroKey   = true;
                zeroKeyIndex = lookup;
            }
        } else if (isLongKey) {
            longKeyTable[lookup] = longKey;

            if (longKey == 0) {
                hasZeroKey   = true;
                zeroKeyIndex = lookup;
            }
        }

        if (isObjectValue) {
            objectValueTable[lookup] = objectValue;

            if (objectValue == null) {
                zeroOrNullValueCount++;
            }
        } else if (isIntValue) {
            intValueTable[lookup] = (int) longValue;

            if (longValue == 0) {
                zeroOrNullValueCount++;
            }
        } else if (isLongValue) {
            longValueTable[lookup] = longValue;

            if (longValue == 0) {
                zeroOrNullValueCount++;
            }
        }

        if (isLastAccessCount) {
            accessTable[lookup] = accessCount.incrementAndGet();
        } else if (isAccessCount) {
            accessTable[lookup] = 1;
        }

        return true;
    }

    /**
     * Single method for removing key / values in multi-value maps.
     */
    protected Object removeMultiVal(long longKey, long longValue,
                                    Object objectKey, Object objectValue,
                                    boolean matchValue) {

        if (objectKey == null) {
            return null;
        }

        int    hash        = comparator.hashCode(objectKey);
        int    index       = hashIndex.getHashIndex(hash);
        int    lookup      = hashIndex.hashTable[index];
        int    lastLookup  = -1;
        int    matchedKey  = -1;
        Object returnValue = null;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            boolean matched = false;

            if (isObjectKey) {
                matched = comparator.equals(objectKeyTable[lookup], objectKey);

                if (matched) {
                    matchedKey = lookup;
                } else {
                    if (matchedKey < 0) {
                        continue;
                    } else {
                        break;
                    }
                }

                if (matchValue) {
                    matched = ObjectComparator.defaultComparator.equals(
                        objectValueTable[lookup], objectValue);

                    if (matched) {
                        objectKeyTable[lookup]   = null;
                        returnValue              = objectValueTable[lookup];
                        objectValueTable[lookup] = null;

                        if (returnValue == null) {
                            zeroOrNullValueCount--;
                        }

                        hashIndex.unlinkNode(index, lastLookup, lookup);

                        return returnValue;
                    } else {
                        continue;
                    }
                } else {
                    objectKeyTable[lookup]   = null;
                    returnValue              = objectValueTable[lookup];
                    objectValueTable[lookup] = null;

                    if (returnValue == null) {
                        zeroOrNullValueCount--;
                    }

                    if (lastLookup > lookup) {
                        lastLookup = lastLookup;
                    }

                    hashIndex.unlinkNode(index, lastLookup, lookup);

                    if (lastLookup < 0) {
                        lookup = hashIndex.hashTable[index];

                        if (lookup < 0) {
                            break;
                        }
                    } else {
                        lookup = lastLookup;
                    }
                }
            }
        }

        return returnValue;
    }

    /**
     * type specific method for Object sets or Object to Object maps
     */
    protected Object removeObject(Object objectKey, boolean removeRow) {

        if (objectKey == null) {
            return null;
        }

        int    hash        = comparator.hashCode(objectKey);
        int    index       = hashIndex.getHashIndex(hash);
        int    lookup      = hashIndex.hashTable[index];
        int    lastLookup  = -1;
        Object returnValue = null;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            if (!comparator.equals(objectKeyTable[lookup], objectKey)) {
                continue;
            }

            returnValue            = objectKeyTable[lookup];
            objectKeyTable[lookup] = null;

            if (accessTable != null) {
                accessTable[lookup] = 0;
            }

            hashIndex.unlinkNode(index, lastLookup, lookup);

            if (isObjectValue) {
                returnValue              = objectValueTable[lookup];
                objectValueTable[lookup] = null;
            }

            if (removeRow) {
                removeRow(lookup);
            }

            return returnValue;
        }

        // not found
        return returnValue;
    }

    /**
     * For object sets using long key attribute of object for equality and
     * hash. Used in org.hsqldb.persist.Cache
     */
    protected Object addOrRemoveObject(long longKey, Object object,
                                       boolean remove) {

        int    hash        = (int) (longKey >>> 32 ^ longKey);
        int    index       = hashIndex.getHashIndex(hash);
        int    lookup      = hashIndex.getLookup(hash);
        int    lastLookup  = -1;
        Object returnValue = null;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            if (comparator.longKey(objectKeyTable[lookup]) == longKey) {
                returnValue = objectKeyTable[lookup];

                break;
            }
        }

        if (lookup >= 0) {
            if (remove) {
                objectKeyTable[lookup] = null;

                hashIndex.unlinkNode(index, lastLookup, lookup);

                if (accessTable != null) {
                    accessTable[lookup] = 0;
                }

                if (minimizeOnEmpty && hashIndex.elementCount == 0) {
                    rehash(initialCapacity);
                }
            } else {
                objectKeyTable[lookup] = object;

                if (isLastAccessCount) {
                    accessTable[lookup] = accessCount.incrementAndGet();
                } else if (isAccessCount) {
                    accessTable[lookup]++;
                }
            }

            return returnValue;
        } else if (remove) {
            return null;
        }

        if (hashIndex.elementCount >= threshold) {
            if (reset()) {
                return addOrRemoveObject(longKey, object, remove);
            } else {
                throw new NoSuchElementException("BaseHashMap");
            }
        }

        lookup                 = hashIndex.linkNode(index, lastLookup);
        objectKeyTable[lookup] = object;

        if (isLastAccessCount) {
            accessTable[lookup] = accessCount.incrementAndGet();
        } else if (isAccessCount) {
            accessTable[lookup] = 1;
        }

        return returnValue;
    }

    protected boolean reset() {

        if (maxCapacity == 0 || maxCapacity > threshold) {
            rehash(hashIndex.linkTable.length * 2);

            return true;
        }

        switch (purgePolicy) {

            case PURGE_ALL :
                clear();

                return true;

            case PURGE_HALF :
                clearToHalf();

                return true;

            case NO_PURGE :
            default :
                return false;
        }
    }

    /**
     * rehash uses existing key and element arrays. key / value pairs are
     * put back into the arrays from the top, removing any gaps. any redundant
     * key / value pairs duplicated at the end of the array are then cleared.
     *
     * newCapacity must be larger or equal to existing number of elements.
     */
    protected void rehash(int newCapacity) {

        int     limitLookup     = hashIndex.newNodePointer;
        boolean oldZeroKey      = hasZeroKey;
        int     oldZeroKeyIndex = zeroKeyIndex;

        if (newCapacity < hashIndex.elementCount) {
            return;
        }

        hashIndex.reset((int) (newCapacity * loadFactor), newCapacity);

        hasZeroKey           = false;
        zeroKeyIndex         = -1;
        zeroOrNullValueCount = 0;
        threshold    = newCapacity;

        for (int lookup = -1;
                (lookup = nextLookup(lookup, limitLookup, oldZeroKey, oldZeroKeyIndex))
                < limitLookup; ) {
            long   longKey     = 0;
            long   longValue   = 0;
            Object objectKey   = null;
            Object objectValue = null;

            if (isObjectKey) {
                objectKey = objectKeyTable[lookup];
            } else if (isIntKey) {
                longKey = intKeyTable[lookup];
            } else {
                longKey = longKeyTable[lookup];
            }

            if (isObjectValue) {
                objectValue = objectValueTable[lookup];
            } else if (isIntValue) {
                longValue = intValueTable[lookup];
            } else if (isLongValue) {
                longValue = longValueTable[lookup];
            }

            if (isMultiValue) {
                addMultiVal(longKey, longValue, objectKey, objectValue);
            } else {
                addOrUpdate(longKey, longValue, objectKey, objectValue);
            }

            if (accessTable != null) {
                accessTable[hashIndex.elementCount - 1] = accessTable[lookup];
            }
        }

        resizeElementArrays(hashIndex.newNodePointer, newCapacity);
    }

    /**
     * resize the arrays containing the key / value data
     */
    private void resizeElementArrays(int dataLength, int newLength) {

        Object temp;
        int    usedLength = newLength > dataLength ? dataLength
                                                   : newLength;

        if (isIntKey) {
            temp        = intKeyTable;
            intKeyTable = new int[newLength];

            System.arraycopy(temp, 0, intKeyTable, 0, usedLength);
        }

        if (isIntValue) {
            temp          = intValueTable;
            intValueTable = new int[newLength];

            System.arraycopy(temp, 0, intValueTable, 0, usedLength);
        }

        if (isLongKey) {
            temp         = longKeyTable;
            longKeyTable = new long[newLength];

            System.arraycopy(temp, 0, longKeyTable, 0, usedLength);
        }

        if (isLongValue) {
            temp           = longValueTable;
            longValueTable = new long[newLength];

            System.arraycopy(temp, 0, longValueTable, 0, usedLength);
        }

        if (objectKeyTable != null) {
            temp           = objectKeyTable;
            objectKeyTable = new Object[newLength];

            System.arraycopy(temp, 0, objectKeyTable, 0, usedLength);
        }

        if (isObjectValue) {
            temp             = objectValueTable;
            objectValueTable = new Object[newLength];

            System.arraycopy(temp, 0, objectValueTable, 0, usedLength);
        }

        if (objectValueTable2 != null) {
            temp              = objectValueTable2;
            objectValueTable2 = new Object[newLength];

            System.arraycopy(temp, 0, objectValueTable2, 0, usedLength);
        }

        if (accessTable != null) {
            temp        = accessTable;
            accessTable = new int[newLength];

            System.arraycopy(temp, 0, accessTable, 0, usedLength);
        }
    }

    /**
     * clear all the key / value data in a range.
     */
    private void clearElementArrays(final int from, final int to) {

        if (intKeyTable != null) {
            Arrays.fill(intKeyTable, from, to, 0);
        } else if (longKeyTable != null) {
            Arrays.fill(longKeyTable, from, to, 0);
        }

        if (objectKeyTable != null) {
            Arrays.fill(objectKeyTable, from, to, null);
        }

        if (intValueTable != null) {
            Arrays.fill(intValueTable, from, to, 0);
        } else if (longValueTable != null) {
            Arrays.fill(longValueTable, from, to, 0);
        } else if (objectValueTable != null) {
            Arrays.fill(objectValueTable, from, to, null);
        }

        if (objectValueTable2 != null) {
            Arrays.fill(objectValueTable2, from, to, null);
        }

        if (accessTable != null) {
            Arrays.fill(accessTable, from, to, 0);
        }
    }

    /**
     * move the elements after a removed key / value pair to fill the gap
     */
    void removeFromElementArrays(int size, int lookup) {

        if (isIntKey) {
            Object array = intKeyTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             size - lookup - 1);

            intKeyTable[size - 1] = 0;
        } else if (isLongKey) {
            Object array = longKeyTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             size - lookup - 1);

            longKeyTable[size - 1] = 0;
        }

        if (objectKeyTable != null) {
            Object array = objectKeyTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             size - lookup - 1);

            objectKeyTable[size - 1] = null;
        }

        if (isIntValue) {
            Object array = intValueTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             size - lookup - 1);

            intValueTable[size - 1] = 0;
        } else if (isLongValue) {
            Object array = longValueTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             size - lookup - 1);

            longValueTable[size - 1] = 0;
        }

        if (isObjectValue) {
            Object array = objectValueTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             size - lookup - 1);

            objectValueTable[size - 1] = null;
        }

        if (objectValueTable2 != null) {
            Object array = objectValueTable2;

            System.arraycopy(array, lookup + 1, array, lookup,
                             size - lookup - 1);

            objectValueTable2[size - 1] = null;
        }

        if (accessTable != null) {
            Object array = accessTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             size - lookup - 1);

            accessTable[size - 1] = 0;
        }
    }

    /**
     * move the elements to create a gap
     */
    void insertIntoElementArrays(int size, int lookup) {

        if (isIntKey) {
            Object array = intKeyTable;

            System.arraycopy(array, lookup, array, lookup + 1, size - lookup);

            intKeyTable[lookup] = 0;
        } else if (isLongKey) {
            Object array = longKeyTable;

            System.arraycopy(array, lookup, array, lookup + 1, size - lookup);

            longKeyTable[lookup] = 0;
        }

        if (objectKeyTable != null) {
            Object array = objectKeyTable;

            System.arraycopy(array, lookup, array, lookup + 1, size - lookup);

            objectKeyTable[lookup] = null;
        }

        if (isIntValue) {
            Object array = intValueTable;

            System.arraycopy(array, lookup, array, lookup + 1, size - lookup);

            intValueTable[lookup] = 0;
        } else if (isLongValue) {
            Object array = longValueTable;

            System.arraycopy(array, lookup, array, lookup + 1, size - lookup);

            longValueTable[lookup] = 0;
        }

        if (isObjectValue) {
            Object array = objectValueTable;

            System.arraycopy(array, lookup, array, lookup + 1, size - lookup);

            objectValueTable[lookup] = null;
        }

        if (objectValueTable2 != null) {
            Object array = objectValueTable2;

            System.arraycopy(array, lookup, array, lookup + 1, size - lookup);

            objectValueTable2[lookup] = null;
        }

        if (accessTable != null) {
            Object array = accessTable;

            System.arraycopy(array, lookup, array, lookup + 1, size - lookup);

            accessTable[lookup] = 0;
        }
    }

    /**
     * find the next lookup in the key/value tables with an entry
     * allows the use of old limit and zero int key attributes
     */
    int nextLookup(int lookup, int limitLookup, boolean hasZeroKey,
                   int zeroKeyIndex) {

        for (++lookup; lookup < limitLookup; lookup++) {
            if (isObjectKey) {
                if (objectKeyTable[lookup] != null) {
                    return lookup;
                }
            } else if (isIntKey) {
                if (intKeyTable[lookup] != 0) {
                    return lookup;
                } else if (hasZeroKey && lookup == zeroKeyIndex) {
                    return lookup;
                }
            } else {
                if (longKeyTable[lookup] != 0) {
                    return lookup;
                } else if (hasZeroKey && lookup == zeroKeyIndex) {
                    return lookup;
                }
            }
        }

        return lookup;
    }

    /**
     * find the next lookup in the key/value tables with an entry
     * uses current limits and zero integer key state
     */
    protected int nextLookup(int lookup) {

        for (++lookup; lookup < hashIndex.newNodePointer; lookup++) {
            if (isObjectKey) {
                if (objectKeyTable[lookup] != null) {
                    return lookup;
                }
            } else if (isIntKey) {
                if (intKeyTable[lookup] != 0) {
                    return lookup;
                } else if (hasZeroKey && lookup == zeroKeyIndex) {
                    return lookup;
                }
            } else {
                if (longKeyTable[lookup] != 0) {
                    return lookup;
                } else if (hasZeroKey && lookup == zeroKeyIndex) {
                    return lookup;
                }
            }
        }

        return -1;
    }

    /**
     * row already freed of key / element
     */
    protected void removeRow(int lookup) {

        int size = hashIndex.newNodePointer;

        if (size == 0) {
            return;
        }

        hashIndex.removeEmptyNode(lookup);
        removeFromElementArrays(size, lookup);
    }

    protected void insertRow(int lookup) {

        if (hashIndex.elementCount >= threshold) {
            reset();
        }

        if (lookup == hashIndex.elementCount) {
            return;
        }

        int size = hashIndex.newNodePointer;

        if (size == 0) {
            return;
        }

        insertIntoElementArrays(size, lookup);
        hashIndex.insertEmptyNode(lookup);
    }

    /**
     * Clear the map completely.
     */
    public void clear() {

        if (hashIndex.modified) {
            if (accessCount != null) {
                accessCount.set(0);
            }

            accessMin            = 0;
            hasZeroKey           = false;
            zeroKeyIndex         = -1;
            zeroOrNullValueCount = 0;

            clearElementArrays(0, hashIndex.newNodePointer);
            hashIndex.clear();

            if (minimizeOnEmpty) {
                rehash(initialCapacity);
            }
        }
    }

    /**
     * Return the max accessCount value for count elements with the lowest
     * access count. Always return at least accessMin + 1
     */
    protected int getAccessCountCeiling(int count, int margin) {
        return ArrayCounter.rank(accessTable, hashIndex.newNodePointer, count,
                                 accessMin, accessCount.get(), margin);
    }

    /**
     * This is called after all elements below count accessCount have been
     * removed
     */
    protected void setAccessCountFloor(int count) {
        accessMin = count;
    }

    /**
     * Clear approximately half elements from the map, starting with
     * those with low accessTable ranking.
     *
     * Only for value maps
     */
    private void clearToHalf() {

        int count  = threshold >> 1;
        int margin = threshold >> 8;

        if (margin < 64) {
            margin = 64;
        }

        int maxlookup  = hashIndex.newNodePointer;
        int accessBase = getAccessCountCeiling(count, margin);

        for (int lookup = 0; lookup < maxlookup; lookup++) {
            Object o = objectKeyTable[lookup];

            if (o != null && accessTable[lookup] < accessBase) {
                removeObject(o, false);
            }
        }

        accessMin = accessBase;

        if (hashIndex.elementCount > threshold - margin) {
            clear();
        }
    }

    protected void resetAccessCount() {

        int accessMax = accessCount.get();

        if (accessMax > 0 && accessMax < ACCESS_MAX) {
            return;
        }

        int limit = hashIndex.getNewNodePointer();

        accessMax = 0;
        accessMin = Integer.MAX_VALUE;

        for (int i = 0; i < limit; i++) {
            int access = accessTable[i];

            if (access == 0) {
                continue;
            }

            access         = (access >>> 2) + 1;
            accessTable[i] = access;

            if (access > accessMax) {
                accessMax = access;
            } else if (access < accessMin) {
                accessMin = access;
            }
        }

        if (accessMin > accessMax) {
            accessMin = accessMax;
        }

        accessCount.set(accessMax);
    }

    protected int capacity() {
        return hashIndex.linkTable.length;
    }

    public int size() {
        return hashIndex.elementCount;
    }

    public boolean isEmpty() {
        return hashIndex.elementCount == 0;
    }

    protected boolean containsKey(Object key) {

        if (key == null) {
            return false;
        }

        if (hashIndex.elementCount == 0) {
            return false;
        }

        int lookup = getLookup(key, comparator.hashCode(key));

        return lookup == -1 ? false
                            : true;
    }

    protected boolean containsKey(int key) {

        if (hashIndex.elementCount == 0) {
            return false;
        }

        int lookup = getLookup(key);

        return lookup == -1 ? false
                            : true;
    }

    protected boolean containsKey(long key) {

        if (hashIndex.elementCount == 0) {
            return false;
        }

        int lookup = getLookup(key);

        return lookup == -1 ? false
                            : true;
    }

    protected boolean containsValue(Object value) {

        int lookup = 0;

        if (hashIndex.elementCount == 0) {
            return false;
        }

        if (value == null) {
            for (; lookup < hashIndex.newNodePointer; lookup++) {
                if (objectValueTable[lookup] == null) {
                    if (isObjectKey) {
                        if (objectKeyTable[lookup] != null) {
                            return true;
                        }
                    } else if (isIntKey) {
                        if (intKeyTable[lookup] != 0) {
                            return true;
                        } else if (hasZeroKey && lookup == zeroKeyIndex) {
                            return true;
                        }
                    } else {
                        if (longKeyTable[lookup] != 0) {
                            return true;
                        } else if (hasZeroKey && lookup == zeroKeyIndex) {
                            return true;
                        }
                    }
                }
            }
        } else {
            for (; lookup < hashIndex.newNodePointer; lookup++) {
                if (value.equals(objectValueTable[lookup])) {
                    return true;
                }
            }
        }

        return false;
    }

    protected boolean containsValue(int value) {

        if (value == 0) {
            return zeroOrNullValueCount > 0;
        }

        for (int lookup = 0; lookup < hashIndex.newNodePointer; lookup++) {
            if (intValueTable[lookup] == value) {
                return true;
            }
        }

        return false;
    }

    protected boolean containsValue(long value) {

        if (value == 0) {
            return zeroOrNullValueCount > 0;
        }

        for (int lookup = 0; lookup < hashIndex.newNodePointer; lookup++) {
            if (longValueTable[lookup] == value) {
                return true;
            }
        }

        return false;
    }

    protected Object[] toArray(boolean keys) {

        Object[] array = new Object[size()];

        return toArray(array, keys);
    }


    protected <T> T[] multiValueKeysToArray(T[] array) {

        int size = this.multiValueKeyCount();
        if (array.length < size) {
            array = (T[]) Array.newInstance(array.getClass().getComponentType(),
                size);
        }

        PrimitiveIterator it = new MultiValueKeyIterator();

        int index = 0;

        while (it.hasNext()) {
            array[index] = (T) it.next();

            index++;
        }

        return array;
    }


    protected <T> T[] toArray(T[] array, boolean keys) {

        if (array.length < size()) {
            array = (T[]) Array.newInstance(array.getClass().getComponentType(),
                size());
        }

        int limit = hashIndex.getNewNodePointer();
        int index = 0;
        Object[] table = keys ? objectKeyTable :
                                objectValueTable;

        for (int i = 0; i < limit; i++) {
            T o = (T) table[i];

            if (o != null) {
                array[index++] = o;
            }
        }

        return array;
    }

    protected int[] toIntArray(int[] array, boolean keys) {

        if (array.length < size()) {
            array = new int[size()];
        }

        PrimitiveIterator it = new BaseHashIterator(keys);

        for (int i = 0; i < array.length; i++) {
            int value = it.nextInt();

            array[i] = value;
        }

        return array;
    }

    protected long[] toLongArray(long[] array, boolean keys) {

        if (array.length < size()) {
            array = new long[size()];
        }

        PrimitiveIterator it = new BaseHashIterator(keys);

        for (int i = 0; i < array.length; i++) {
            long value = it.nextInt();

            array[i] = value;
        }

        return array;
    }

    /**
     * Iterator for accessing the values for a single key in MultiValueHashMap
     * Currently only for object maps
     */
    protected class ValueCollectionIterator implements PrimitiveIterator {

        int    lookup = -1;
        Object key;

        ValueCollectionIterator(Object key, int lookup) {
            this.key    = key;
            this.lookup = lookup;
        }

        public boolean hasNext() {
            return lookup != -1;
        }

        public Object next() throws NoSuchElementException {

            if (lookup == -1) {
                throw new NoSuchElementException();
            }

            Object value = BaseHashMap.this.objectValueTable[lookup];

            lookup = BaseHashMap.this.hashIndex.getNextLookup(lookup);

            if (lookup != -1) {
                Object nextKey = BaseHashMap.this.objectKeyTable[lookup];

                if (!comparator.equals(nextKey, key)) {
                    lookup = -1;
                }
            }

            return value;
        }

        public int nextInt() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public long nextLong() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public void remove() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }
    }

    protected class MultiValueKeyIterator implements PrimitiveIterator {

        int     index  = 0;
        int     lookup = -1;
        boolean removed;
        Object  oldKey;

        public MultiValueKeyIterator() {

            if (hashIndex.elementCount > 0) {
                toNextLookup();
            }
        }

        private void toNextLookup() {

            for (; index < hashIndex.hashTable.length; ) {
                if (hashIndex.hashTable[index] < 0) {
                    index++;

                    continue;
                }

                if (lookup < 0) {
                    lookup = hashIndex.hashTable[index];
                } else {
                    lookup = hashIndex.getNextLookup(lookup);
                }

                if (lookup < 0) {
                    index++;

                    continue;
                }

                if (comparator.equals(oldKey, objectKeyTable[lookup])) {
                    continue;
                }

                break;
            }
        }

        public boolean hasNext() {
            return lookup != -1;
        }

        public Object next() throws NoSuchElementException {

            if (lookup < 0) {
                throw new NoSuchElementException("Hash Iterator");
            }

            Object value = objectKeyTable[lookup];

            toNextLookup();

            oldKey = value;

            return value;
        }

        public int nextInt() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public long nextLong() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public void remove() throws NoSuchElementException {

            removeMultiVal(0, 0, oldKey, null, false);

            oldKey = null;
        }
    }

    /**
     * Iterator returns Object, int or long and is used both for keys and
     * values
     */
    protected class BaseHashIterator implements PrimitiveIterator {

        protected boolean keys;
        protected int     lookup = -1;
        protected int     counter;
        protected boolean removed;

        /**
         * default is iterator for values
         */
        public BaseHashIterator() {

        }

        public BaseHashIterator(boolean keys) {

            this.keys = keys;

            if (!keys && isNoValue) {
                throw new RuntimeException("Hash Iterator");
            }
        }

        public void reset() {

            this.lookup  = -1;
            this.counter = 0;
            this.removed = false;
        }

        public boolean hasNext() {
            return counter < hashIndex.elementCount;
        }

        public Object next() throws NoSuchElementException {

            if (keys) {
                if (isIntKey) {
                    return nextInt();
                } else if (isLongKey) {
                    return nextLong();
                }
            } else {
                if (isIntValue) {
                    return nextInt();
                } else if (isLongValue) {
                    return nextLong();
                }
            }

            removed = false;

            if (hasNext()) {
                counter++;

                lookup = nextLookup(lookup);

                if (keys) {
                    return objectKeyTable[lookup];
                } else {
                    return objectValueTable[lookup];
                }
            }

            throw new NoSuchElementException("Hash Iterator");
        }

        public int nextInt() throws NoSuchElementException {

            if ((keys && !isIntKey) || (!keys && !isIntValue)) {
                throw new NoSuchElementException("Hash Iterator");
            }

            removed = false;

            if (hasNext()) {
                counter++;

                lookup = nextLookup(lookup);

                if (keys) {
                    return intKeyTable[lookup];
                } else {
                    return intValueTable[lookup];
                }
            }

            throw new NoSuchElementException("Hash Iterator");
        }

        public long nextLong() throws NoSuchElementException {

            if ((keys && !isLongKey) || (!keys && !isLongValue)) {
                throw new NoSuchElementException("Hash Iterator");
            }

            removed = false;

            if (hasNext()) {
                counter++;

                lookup = nextLookup(lookup);

                return keys ? longKeyTable[lookup]
                            : longValueTable[lookup];
            }

            throw new NoSuchElementException("Hash Iterator");
        }

        public void remove() throws NoSuchElementException {

            if (removed) {
                throw new NoSuchElementException("Hash Iterator");
            }

            counter--;

            removed = true;

            if (BaseHashMap.this.isObjectKey) {
                if (isMultiValue) {
                    removeMultiVal(0, 0, objectKeyTable[lookup],
                                   objectValueTable[lookup], true);
                } else {
                    BaseHashMap.this.remove(0, 0, objectKeyTable[lookup],
                                            null, false, true);
                }
            } else if (isIntKey) {
                BaseHashMap.this.remove(intKeyTable[lookup], 0, null, null,
                                        false, true);
            } else {
                BaseHashMap.this.remove(longKeyTable[lookup], 0, null, null,
                                        false, true);
            }

            if (isList) {
                lookup--;
            }
        }

        public int getAccessCount() {

            if (removed || accessTable == null) {
                throw new NoSuchElementException();
            }

            return accessTable[lookup];
        }

        public void setAccessCount(int count) {

            if (removed || accessTable == null) {
                throw new NoSuchElementException();
            }

            accessTable[lookup] = count;
        }

        public int getLookup() {
            return lookup;
        }
    }

    public BaseHashMap clone() {

        BaseHashMap copy = null;

        try {
            copy = (BaseHashMap) super.clone();
        } catch (CloneNotSupportedException e) {}

        copy.hashIndex = hashIndex.clone();

        if (intKeyTable != null) {
            copy.intKeyTable = intKeyTable.clone();
        }

        if (objectKeyTable != null) {
            copy.objectKeyTable = objectKeyTable.clone();
        }

        if (longKeyTable != null) {
            copy.longKeyTable = longKeyTable.clone();
        }

        if (intValueTable != null) {
            copy.intValueTable = intValueTable.clone();
        }

        if (objectValueTable != null) {
            copy.objectValueTable = objectValueTable.clone();
        }

        if (longValueTable != null) {
            copy.longValueTable = longValueTable.clone();
        }

        if (accessTable != null) {
            copy.accessTable = accessTable.clone();
        }

        if (objectValueTable2 != null) {
            copy.objectValueTable2 = objectValueTable2.clone();
        }

        return copy;
    }

    BaseHashMap duplicate() {
        return null;
    }
}
