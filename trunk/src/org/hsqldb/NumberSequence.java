/* Copyright (c) 2001-2007, The HSQL Development Group
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


package org.hsqldb;

import java.math.BigDecimal;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.rights.Grantee;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;
import org.hsqldb.lib.OrderedHashSet;

/**
 * Maintains a sequence of numbers.
 *
 * @author  fredt@users
 * @version  1.9.0
 * @since 1.7.2
 */
public class NumberSequence implements SchemaObject {

    HsqlName name;

    // present value
    private long currValue;

    // last value
    private long lastValue;

    // limit state
    private boolean limitReached;

    // original start value - used in CREATE and ALTER commands
    private long    startValue;
    private long    minValue;
    private long    maxValue;
    private long    increment;
    private Type    dataType;
    private boolean isCycle;
    private boolean isAlways;

    private NumberSequence() {}

    public NumberSequence(HsqlName name, Type type) throws HsqlException {
        setDefaults(name, type);
    }

    public void setDefaults(HsqlName name, Type type) throws HsqlException {

        this.name = name;
        dataType  = type;
        this.name = name;
        dataType  = type;

        long min;
        long max;

        switch (dataType.type) {

            case Types.SQL_SMALLINT :
                max = Short.MAX_VALUE;
                min = Short.MIN_VALUE;
                break;

            case Types.SQL_INTEGER :
                max = Integer.MAX_VALUE;
                min = Integer.MIN_VALUE;
                break;

            case Types.SQL_BIGINT :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                if (type.scale() == 0) {
                    max = Long.MAX_VALUE;
                    min = Long.MIN_VALUE;

                    break;
                }
            default :
                throw Trace.error(Trace.WRONG_DATA_TYPE);
        }

        minValue  = min;
        maxValue  = max;
        increment = 1;
    }

    /**
     * constructor with initial value and increment;
     */
    public NumberSequence(HsqlName name, long value, long increment,
                          Type type) throws HsqlException {

        this(name, type);

        setStartValue(value);
        setIncrement(increment);
    }

    public HsqlName getName() {
        return name;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }

    public void compile(Session session) {}

    public Type getType() {
        return dataType;
    }

    public long getIncrement() {
        return increment;
    }

    synchronized long getStartValue() {
        return startValue;
    }

    synchronized long getMinValue() {
        return minValue;
    }

    synchronized long getMaxValue() {
        return maxValue;
    }

    synchronized boolean isCycle() {
        return isCycle;
    }

    synchronized boolean isAlways() {
        return isAlways;
    }

    synchronized boolean hasDefaultMinMax() {

        long min;
        long max;

        switch (dataType.type) {

            case Types.SQL_SMALLINT :
                max = Short.MAX_VALUE;
                min = Short.MIN_VALUE;
                break;

            case Types.SQL_INTEGER :
                max = Integer.MAX_VALUE;
                min = Integer.MIN_VALUE;
                break;

            case Types.SQL_BIGINT :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberSequence");
        }

        return minValue == min && maxValue == max;
    }

    synchronized void setStartValue(long value) throws HsqlException {

        if (value < minValue || value > maxValue) {
            throw Trace.error(Trace.SQL_INVALID_SEQUENCE_PARAMETER);
        }

        startValue = value;
        currValue  = lastValue = startValue;
    }

    synchronized void setMinValue(long value) throws HsqlException {

        checkInTypeRange(value);

        if (value >= maxValue || currValue < value) {
            throw Trace.error(Trace.SQL_INVALID_SEQUENCE_PARAMETER);
        }

        minValue = value;
    }

    synchronized void setDefaultMinValue() throws HsqlException {
        minValue = getDefaultMinOrMax(false);
    }

    synchronized void setMaxValue(long value) throws HsqlException {

        checkInTypeRange(value);

        if (value <= minValue || currValue > value) {
            throw Trace.error(Trace.SQL_INVALID_SEQUENCE_PARAMETER);
        }

        maxValue = value;
    }

    synchronized void setDefaultMaxValue() throws HsqlException {
        maxValue = getDefaultMinOrMax(true);
    }

    synchronized void setIncrement(long value) throws HsqlException {

        if (value < Short.MIN_VALUE / 2 || value > Short.MAX_VALUE / 2) {
            throw Trace.error(Trace.SQL_INVALID_SEQUENCE_PARAMETER);
        }

        increment = value;
    }

    synchronized void setStartValueNoCheck(long value) throws HsqlException {

        checkInTypeRange(value);

        startValue = value;
        currValue  = lastValue = startValue;
    }

    synchronized void setMinValueNoCheck(long value) throws HsqlException {

        checkInTypeRange(value);

        minValue = value;
    }

    synchronized void setMaxValueNoCheck(long value) throws HsqlException {

        checkInTypeRange(value);

        maxValue = value;
    }

    synchronized void setCycle(boolean value) {
        isCycle = value;
    }

    synchronized void setAlways(boolean value) {
        isAlways = value;
    }

    private long getDefaultMinOrMax(boolean isMax) throws HsqlException {

        long min;
        long max;

        switch (dataType.type) {

            case Types.SQL_SMALLINT :
                max = Short.MAX_VALUE;
                min = Short.MIN_VALUE;
                break;

            case Types.SQL_INTEGER :
                max = Integer.MAX_VALUE;
                min = Integer.MIN_VALUE;
                break;

            case Types.SQL_BIGINT :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberSequence");
        }

        return isMax ? max
                     : min;
    }

    private void checkInTypeRange(long value) throws HsqlException {

        long min;
        long max;

        switch (dataType.type) {

            case Types.SQL_SMALLINT :
                max = Short.MAX_VALUE;
                min = Short.MIN_VALUE;
                break;

            case Types.SQL_INTEGER :
                max = Integer.MAX_VALUE;
                min = Integer.MIN_VALUE;
                break;

            case Types.SQL_BIGINT :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "NumberSequence");
        }

        if (value < min || value > max) {
            throw Trace.error(Trace.SQL_INVALID_SEQUENCE_PARAMETER);
        }
    }

    synchronized void checkValues() throws HsqlException {

        if (minValue >= maxValue || startValue < minValue
                || startValue > maxValue || currValue < minValue
                || currValue > maxValue) {
            throw Trace.error(Trace.SQL_INVALID_SEQUENCE_PARAMETER);
        }
    }

    synchronized NumberSequence duplicate() {

        NumberSequence copy = new NumberSequence();

        copy.name       = name;
        copy.startValue = startValue;
        copy.currValue  = currValue;
        copy.lastValue  = lastValue;
        copy.increment  = increment;
        copy.dataType   = dataType;
        copy.minValue   = minValue;
        copy.maxValue   = maxValue;
        copy.isCycle    = isCycle;
        copy.isAlways   = isAlways;

        return copy;
    }

    synchronized void reset(NumberSequence other) {

        name       = other.name;
        startValue = other.startValue;
        currValue  = other.currValue;
        lastValue  = other.lastValue;
        increment  = other.increment;
        dataType   = other.dataType;
        minValue   = other.minValue;
        maxValue   = other.maxValue;
        isCycle    = other.isCycle;
        isAlways   = other.isAlways;
    }

    /**
     * getter for a given value
     */
    synchronized long userUpdate(long value) throws HsqlException {

        if (value == currValue) {
            currValue += increment;

            return value;
        }

        if (increment > 0) {
            if (value > currValue) {
                currValue = currValue
                            + ((value - currValue + increment) / increment)
                              * increment;
            }
        } else {
            if (value < currValue) {
                currValue = currValue
                            + ((value - currValue + increment) / increment)
                              * increment;
            }
        }

        return value;
    }

    /**
     * Updates are necessary for text tables
     * For memory tables, the logged and scripted RESTART WITH will override
     * this.
     * No checks as values may have overridden the sequnece defaults
     */
    synchronized long systemUpdate(long value) {

        if (value == currValue) {
            currValue += increment;

            return value;
        }

        if (increment > 0) {
            if (value > currValue) {
                currValue = value + increment;
            }
        } else {
            if (value < currValue) {
                currValue = value + increment;
            }
        }

        return value;
    }

    synchronized Object getValueObject() throws HsqlException {

        long   value = getValue();
        Object result;

        switch (dataType.type) {

            default :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
                result = ValuePool.getInt((int) value);
                break;

            case Types.SQL_BIGINT :
                result = ValuePool.getLong(value);
                break;

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                result = ValuePool.getBigDecimal(new BigDecimal(value));
                break;
        }

        return result;
    }

    /**
     * principal getter for the next sequence value
     */
    synchronized long getValue() throws HsqlException {

        if (limitReached) {
            throw Trace.error(Trace.DATA_SEQUENCE_GENERATOR_LIMIT_EXCEEDED);
        }

        long nextValue;

        if (increment > 0) {
            if (currValue > maxValue - increment) {
                if (isCycle) {
                    nextValue = minValue;
                } else {
                    limitReached = true;
                    nextValue    = minValue;
                }
            } else {
                nextValue = currValue + increment;
            }
        } else {
            if (currValue < minValue - increment) {
                if (isCycle) {
                    nextValue = maxValue;
                } else {
                    limitReached = true;
                    nextValue    = minValue;
                }
            } else {
                nextValue = currValue + increment;
            }
        }

        long result = currValue;

        currValue = nextValue;

        return result;
    }

    /**
     * reset to start value
     */
    synchronized void reset() {

        // no change if called before getValue() or called twice
        lastValue = currValue = startValue;
    }

    /**
     * get next value without incrementing
     */
    synchronized public long peek() {
        return currValue;
    }

    /**
     * reset the wasUsed flag
     */
    synchronized boolean resetWasUsed() {
        boolean result = lastValue != currValue;
        lastValue = currValue;
        return result;
    }

    /**
     * reset to new initial value
     */
    synchronized public void reset(long value) throws HsqlException {

        if (value < minValue || value > maxValue) {
            throw Trace.error(Trace.SQL_INVALID_SEQUENCE_PARAMETER);
        }

        startValue = currValue = lastValue = value;
    }
}
