/* Copyright (c) 2001-2019, The HSQL Development Group
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

import org.hsqldb.lib.OrderedIntHashSet;

/**
 * Enumerate expression operation types<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.0
 * @since 1.9.0
 */
public interface OpTypes {

    int NONE                       = 0,
        VALUE                      = 1,      // constant value
        COLUMN                     = 2,      // references
        COALESCE                   = 3,
        DEFAULT                    = 4,
        SIMPLE_COLUMN              = 5,
        VARIABLE                   = 6,
        PARAMETER                  = 7,
        DYNAMIC_PARAM              = 8,
        TRANSITION_VARIABLE        = 9,
        DIAGNOSTICS_VARIABLE       = 10,
        ASTERISK                   = 11,
        SEQUENCE                   = 12,
        SEQUENCE_CURRENT           = 13,
        ROWNUM                     = 14,
        ARRAY                      = 19,
        MULTISET                   = 20,
        SCALAR_SUBQUERY            = 21,     // query based row or table
        ROW_SUBQUERY               = 22,
        TABLE_SUBQUERY             = 23,
        RECURSIVE_SUBQUERY         = 24,
        ROW                        = 25,     // rows
        VALUELIST                  = 26,
        FUNCTION                   = 27,
        SQL_FUNCTION               = 28,
        STATE_FUNCTION             = 29,
        TABLE                      = 30,
        NEGATE                     = 31,     // arithmetic operations
        ADD                        = 32,
        SUBTRACT                   = 33,
        MULTIPLY                   = 34,
        DIVIDE                     = 35,
        CONCAT                     = 36,     // concatenation
        LIKE_ARG                   = 37,
        CASEWHEN_COALESCE          = 38,
        IS_NOT_NULL                = 39,     // logical - comparison
        EQUAL                      = 40,
        GREATER_EQUAL              = 41,
        GREATER_EQUAL_PRE          = 42,
        GREATER                    = 43,
        SMALLER                    = 44,
        SMALLER_EQUAL              = 45,
        NOT_EQUAL                  = 46,
        IS_NULL                    = 47,
        NOT                        = 48,     // logical operations
        AND                        = 49,
        OR                         = 50,
        ALL_QUANTIFIED             = 51,     // logical - quantified comparison
        ANY_QUANTIFIED             = 52,
        LIKE                       = 53,     // logical - predicates
        IN                         = 54,
        EXISTS                     = 55,
        OVERLAPS                   = 56,
        PERIOD                     = 57,
        PERIOD_CHECK               = 58,
        RANGE_CONTAINS             = 59,
        RANGE_EQUALS               = 60,
        RANGE_OVERLAPS             = 61,
        RANGE_PRECEDES             = 62,
        RANGE_SUCCEEDS             = 63,
        RANGE_IMMEDIATELY_PRECEDES = 64,
        RANGE_IMMEDIATELY_SUCCEEDS = 65,
        UNIQUE                     = 66,
        NOT_DISTINCT               = 67,
        MATCH_SIMPLE               = 68,
        MATCH_PARTIAL              = 69,
        MATCH_FULL                 = 70,
        MATCH_UNIQUE_SIMPLE        = 71,
        MATCH_UNIQUE_PARTIAL       = 72,
        MATCH_UNIQUE_FULL          = 73,
        COUNT                      = 74,     // aggregate functions
        SUM                        = 75,
        MIN                        = 76,
        MAX                        = 77,
        AVG                        = 78,
        EVERY                      = 79,
        SOME                       = 80,
        STDDEV_POP                 = 81,
        STDDEV_SAMP                = 82,
        VAR_POP                    = 83,
        VAR_SAMP                   = 84,
        ARRAY_AGG                  = 85,
        GROUP_CONCAT               = 86,
        PREFIX                     = 87,
        MEDIAN                     = 88,
        CONCAT_WS                  = 89,
        CAST                       = 90,     // other operations
        ZONE_MODIFIER              = 91,
        CASEWHEN                   = 92,
        ORDER_BY                   = 93,
        LIMIT                      = 94,
        ALTERNATIVE                = 95,
        MULTICOLUMN                = 96,
        USER_AGGREGATE             = 98,
        ARRAY_ACCESS               = 99,
        ARRAY_SUBQUERY             = 100,
        GROUPING                   = 101;    // grouping function
    //J-
    int[] aggOpTypes = new int[] {

        OpTypes.COUNT,
        OpTypes.AVG,
        OpTypes.MAX,
        OpTypes.MIN,
        OpTypes.SUM,
        OpTypes.EVERY,
        OpTypes.SOME,
        OpTypes.STDDEV_POP,
        OpTypes.STDDEV_SAMP,
        OpTypes.VAR_POP,
        OpTypes.VAR_SAMP,
        OpTypes.ARRAY_AGG,
        OpTypes.USER_AGGREGATE,
        OpTypes.GROUP_CONCAT,
        OpTypes.MEDIAN,
    };

    int[] columnOpTypes = new int[]{ OpTypes.COLUMN };
    int[] subqueryOpTypes = new int[] {
        OpTypes.ROW_SUBQUERY,
        OpTypes.TABLE_SUBQUERY
    };

    int[] functionOpTypes = new int[] {
        OpTypes.SQL_FUNCTION,
        OpTypes.FUNCTION
    };

    int[] sequenceOpTypes =  new int[] {
        OpTypes.ROWNUM,
        OpTypes.SEQUENCE
    };
    //J+
    OrderedIntHashSet emptyExpressionSet   = new OrderedIntHashSet();
    OrderedIntHashSet aggregateFunctionSet = new OrderedIntHashSet(aggOpTypes);
    OrderedIntHashSet columnExpressionSet =
        new OrderedIntHashSet(columnOpTypes);
    OrderedIntHashSet subqueryExpressionSet =
        new OrderedIntHashSet(subqueryOpTypes);
    OrderedIntHashSet subqueryAggregateExpressionSet =
        new OrderedIntHashSet(subqueryOpTypes, aggOpTypes);
    OrderedIntHashSet functionExpressionSet =
        new OrderedIntHashSet(functionOpTypes);
    OrderedIntHashSet sequenceExpressionSet =
        new OrderedIntHashSet(sequenceOpTypes);
}
