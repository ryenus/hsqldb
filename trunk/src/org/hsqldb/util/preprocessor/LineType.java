/* Copyright (c) 2001-2022, The HSQL Development Group
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


package org.hsqldb.util.preprocessor;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/*
 * $Id$
 */
/**
 * Static methods and constants to decode Preprocessor line types.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 2.7.0
 * @since 1.8.1
 */
@SuppressWarnings("ClassWithoutLogger")
public class LineType {

    //
    public static final int UNKNOWN = 0;
    public static final int DEF = 1;
    public static final int DEFINE = 1;
    public static final int ELIF = 2;
    public static final int ELIFDEF = 3;
    public static final int ELIFNDEF = 4;
    public static final int ELSE = 5;
    public static final int ENDIF = 6;
    public static final int ENDINCLUDE = 7;
    public static final int HIDDEN = 8;
    public static final int IF = 9;
    public static final int IFDEF = 10;
    public static final int IFNDEF = 11;
    public static final int INCLUDE = 12;
    public static final int UNDEF = 13;
    public static final int UNDEFINE = 13;
    public static final int VISIBLE = 14;

    //
    private static Map<String, Integer> directives;
    private static Map<Integer, String> labels;

    static {
        init();
    }

    public static String label(final Integer key) {
        return labels.get(key);
    }

    public static Integer id(final String key) {
        return directives.get(key);
    }

    private static void init() {

        directives = new HashMap<>(23);
        labels = new HashMap<>(23);
        final Field[] fields = LineType.class.getDeclaredFields();
        for (int i = 0, j = 0; i < fields.length; i++) {
            final Field field = fields[i];

            if (field.getType().equals(Integer.TYPE)) {
                final String label = field.getName();

                try {
                    int value = field.getInt(null);

                    labels.put(value, label);

                    switch (value) {
                        case VISIBLE:
                        case HIDDEN: {
                            // ignore
                            break;
                        }
                        default: {
                            final String key = Line.DIRECTIVE_PREFIX
                                    + label.toLowerCase(Locale.ENGLISH);

                            directives.put(key, new Integer(value));

                            break;
                        }
                    }

                } catch (IllegalArgumentException | IllegalAccessException ignored) {
                    // ex.printStackTrace();
                }
            }
        }
    }

    private LineType() {
        throw new AssertionError("Pure Utiluity Class");
    }
}
