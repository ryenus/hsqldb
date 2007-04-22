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


package org.hsqldb.types;

import org.hsqldb.HsqlException;
import org.hsqldb.Trace;

public class IntervalSecondData {

    final int units;
    int       nanos;

    public static IntervalSecondData newIntervalDay(int days,
            IntervalType type) throws HsqlException {
        return new IntervalSecondData(days * 24 * 60 * 60, 0, type);
    }

    public static IntervalSecondData newIntervalHour(int hours,
            IntervalType type) throws HsqlException {
        return new IntervalSecondData(hours * 60 * 60, 0, type);
    }

    public static IntervalSecondData newIntervalMinute(int minutes,
            IntervalType type) throws HsqlException {
        return new IntervalSecondData(minutes * 60, 0, type);
    }

    public static IntervalSecondData newIntervalSeconds(int seconds,
            IntervalType type) throws HsqlException {
        return new IntervalSecondData(seconds, 0, type);
    }

    public IntervalSecondData(long seconds, int nanos,
                              IntervalType type) throws HsqlException {

        if (seconds >= type.getIntervalValueLimit()) {

            // todo - SQL message interval field overflow
            // data exception interval field overflow.
            throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
        }

        this.units = (int) seconds;
        this.nanos = nanos;
    }

    /**
     * normalise is a marker, values are always normalised
     */
    public IntervalSecondData(long seconds, long nanos, IntervalType type,
                              boolean normalise) throws HsqlException {

        if (nanos >= DateTimeType.limitNanoseconds) {
            long carry = nanos / DateTimeType.limitNanoseconds;

            nanos   = nanos % DateTimeType.limitNanoseconds;
            seconds += carry;
        } else if (nanos <= -DateTimeType.limitNanoseconds) {
            long carry = -nanos / DateTimeType.limitNanoseconds;

            nanos   = -(-nanos % DateTimeType.limitNanoseconds);
            seconds -= carry;
        }

        if (seconds > 0 && nanos < 0) {
            nanos += DateTimeType.limitNanoseconds;

            seconds--;
        } else if (seconds < 0 && nanos > 0) {
            nanos -= DateTimeType.limitNanoseconds;

            seconds++;
        }

        if (seconds >= type.getIntervalValueLimit()) {

            // todo - SQL message precision exceeded
            throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
        }

        this.units = (int) seconds;
        this.nanos = (int) nanos;
    }

    public boolean equals(Object other) {

        if (other instanceof IntervalSecondData) {
            return units == ((IntervalSecondData) other).units
                   && nanos == ((IntervalSecondData) other).nanos;
        }

        return false;
    }

    public int hashCode() {
        return (int) units ^ nanos;
    }

    public int compareTo(IntervalSecondData b) {

        long diff = units - b.units;

        if (diff == 0) {
            return (nanos - b.nanos) > 0 ? 1
                                         : -1;
        } else {
            return diff > 0 ? 1
                            : -1;
        }
    }

    public int getSeconds() {
        return units;
    }

    public int getNanos() {
        return nanos;
    }

    public String toString() {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "IntervalSecondData");
    }
}
