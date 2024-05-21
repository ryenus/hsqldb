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


package org.hsqldb.types;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Formats numbers with the given pattern.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 2.7.3
 */
public class NumberFormat {

    final String pattern;
    int          patternSigDigitCount;
    int          patternFracCount;
    boolean      hasPeriod;
    boolean      blankMode;
    boolean      fillMode;
    boolean      signStart;
    boolean      signEnd;
    boolean      signEndMinus;
    boolean      signBrackets;

    public NumberFormat(String pattern) {

        this.pattern = pattern;

        int patternLength = pattern.length();

        for (int i = 0; i < patternLength; i++) {
            switch (pattern.charAt(i)) {

                case 'B' :
                    if (i == 0) {
                        blankMode = true;
                    } else {
                        throw Error.error(ErrorCode.X_22523);
                    }

                    break;

                case 'F' :
                    if (i == 0
                            && patternLength > 1
                            && pattern.charAt(1) == 'M') {
                        fillMode = true;

                        i++;
                    } else {
                        throw Error.error(ErrorCode.X_22523);
                    }

                    break;

                case 'M' :
                    if (!signStart
                            && i == patternLength - 2
                            && pattern.charAt(i + 1) == 'I') {
                        signEnd      = true;
                        signEndMinus = true;

                        i++;
                    } else {
                        throw Error.error(ErrorCode.X_22523);
                    }

                    break;

                case 'P' :
                    if (!signStart
                            && i == patternLength - 2
                            && pattern.charAt(i + 1) == 'R') {
                        signBrackets = true;

                        i++;
                    } else {
                        throw Error.error(ErrorCode.X_22523);
                    }

                    break;

                case '.' :
                    if (hasPeriod) {
                        throw Error.error(ErrorCode.X_22523);
                    }

                    hasPeriod = true;
                    break;

                case ',' :
                    if (hasPeriod) {
                        throw Error.error(ErrorCode.X_22523);
                    }

                    break;

                case 'S' :
                    if (i == 0) {
                        signStart = true;
                    } else if (!signStart && i == patternLength - 1) {
                        signEnd = true;
                    } else {
                        throw Error.error(ErrorCode.X_22523);
                    }

                    break;

                case '9' :
                case '0' :
                    if (hasPeriod) {
                        patternFracCount++;
                    } else {
                        patternSigDigitCount++;
                    }
            }
        }
    }

    public String format(Number num) {

        BigDecimal number;

        if (num instanceof BigDecimal) {
            number = (BigDecimal) num;
        } else if (num instanceof Double) {
            number = BigDecimal.valueOf(num.doubleValue());
        } else {
            number = BigDecimal.valueOf(num.longValue());
        }

        boolean isNegative = number.signum() < 0;

        number = number.setScale(patternFracCount, RoundingMode.HALF_EVEN);
        number = number.abs();

        String numberString = number.toPlainString();
        int intPrecision = numberString.length() - patternFracCount
                           - (patternFracCount == 0
                              ? 0
                              : 1);

        if (patternSigDigitCount < intPrecision) {
            return "#";
        }

        StringBuilder sb = new StringBuilder();

        if (blankMode && number.signum() == 0) {
            int count = patternSigDigitCount + patternFracCount
                        + (patternFracCount == 0
                           ? 0
                           : 1);

            for (int i = 0; i < count; i++) {
                sb.append(' ');
            }

            return sb.toString();
        }

        int     digitCount = 0;
        int     skipCount  = patternSigDigitCount - intPrecision;
        boolean zeroMode   = false;
        boolean signAdded  = false;

        for (int i = 0; i < pattern.length(); i++) {
            switch (pattern.charAt(i)) {

                case '9' :
                    if (digitCount < skipCount) {
                        if (!fillMode) {
                            if (zeroMode) {
                                sb.append('0');
                            } else {
                                sb.append(' ');
                            }
                        }
                    } else if (digitCount == skipCount) {
                        if (isNegative && !signAdded) {
                            appendSignStart(sb, isNegative);

                            signAdded = true;
                        }

                        char c = numberString.charAt(digitCount - skipCount);

                        if (hasPeriod && c == '0') {
                            c = ' ';
                        }

                        sb.append(c);
                    } else {
                        char c = numberString.charAt(digitCount - skipCount);

                        sb.append(c);
                    }

                    digitCount++;
                    break;

                case '0' :
                    if (isNegative && !signAdded) {
                        appendSignStart(sb, isNegative);

                        signAdded = true;
                    }

                    if (digitCount < skipCount) {
                        zeroMode = true;

                        sb.append('0');
                    } else {
                        char c = numberString.charAt(digitCount - skipCount);

                        sb.append(c);
                    }

                    digitCount++;
                    break;

                case '.' :
                    sb.append('.');

                    digitCount++;
                    break;

                case ',' :
                    if (digitCount > skipCount) {
                        sb.append(',');
                    }

                    break;
            }
        }

        if (fillMode && hasPeriod) {
            for (int i = sb.length() - 1; i >= 0; i--) {
                if (sb.charAt(i) == '0') {
                    sb.setLength(i);
                } else {
                    break;
                }
            }
        }

        appendSignEnd(sb, isNegative);

        return sb.toString();
    }

    void appendSignStart(StringBuilder sb, boolean isNegative) {

        if (signBrackets) {
            if (isNegative) {
                sb.append('<');
            } else {
                sb.append(' ');
            }
        } else if (signStart) {
            if (isNegative) {
                sb.append('-');
            } else {
                sb.append('+');
            }
        } else if (!signEnd) {
            if (isNegative) {
                sb.append('-');
            } else if (!fillMode) {
                sb.append(' ');
            }
        }
    }

    void appendSignEnd(StringBuilder sb, boolean isNegative) {

        if (signEnd) {
            if (isNegative) {
                sb.append('-');
            } else {
                if (signEndMinus) {
                    sb.append(' ');
                } else {
                    sb.append('+');
                }
            }
        } else if (signBrackets) {
            if (isNegative) {
                sb.append('>');
            } else {
                sb.append(' ');
            }
        }
    }
}
