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

public class AggregateFunction extends Expression {

    AggregateFunction(int type, boolean distinct, Expression e) {
        super(type, distinct, e);
    }

    public Object updateAggregatingValue(Session session,
                                         Object currValue)
                                         throws HsqlException {

        if (currValue == null) {
            currValue = new SetFunction(exprType, eArg.dataType,
                                        isDistinctAggregate);
        }

        Object newValue = eArg.exprType == ASTERISK ? INTEGER_1
                                                    : eArg.getValue(session);

        ((SetFunction) currValue).add(session, newValue);

        return currValue;
    }

    /**
     * Get the result of a SetFunction or an ordinary value
     *
     * @param currValue instance of set function or value
     * @param session context
     * @return object
     *
     * @throws HsqlException
     */
    public Object getAggregatedValue(Session session,
                                     Object currValue) throws HsqlException {

        if (currValue == null) {
            return exprType == COUNT ? INTEGER_0
                                     : null;
        }

        return ((SetFunction) currValue).getValue();
    }
}
