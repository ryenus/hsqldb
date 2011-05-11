/* Copyright (c) 2001-2010, The HSQL Development Group
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

import org.hsqldb.types.Charset;
import org.hsqldb.types.Type;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.Types;
import org.hsqldb.types.UserTypeModifier;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.CharacterType;
import org.hsqldb.HsqlNameManager.HsqlName;

public class TypeInvariants {

    public static final Charset SQL_TEXT;
    public static final Charset SQL_IDENTIFIER_CHARSET;
    public static final Charset SQL_CHARACTER;
    public static final Charset ASCII_GRAPHIC;    // == GRAPHIC_IRV
    public static final Charset GRAPHIC_IRV;
    public static final Charset ASCII_FULL;       // == ISO8BIT
    public static final Charset ISO8BIT;
    public static final Charset LATIN1;
    public static final Charset UTF32;
    public static final Charset UTF16;
    public static final Charset UTF8;

    static {
        HsqlName name;

        name = HsqlNameManager.newInfoSchemaObjectName("SQL_TEXT", false,
                SchemaObject.CHARSET);
        SQL_TEXT = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("SQL_IDENTIFIER",
                false, SchemaObject.CHARSET);
        SQL_IDENTIFIER_CHARSET = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("SQL_CHARACTER", false,
                SchemaObject.CHARSET);
        SQL_CHARACTER = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("LATIN1", false,
                SchemaObject.CHARSET);
        LATIN1 = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("ASCII_GRAPHIC", false,
                SchemaObject.CHARSET);
        ASCII_GRAPHIC = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("GRAPHIC_IRV", false,
                SchemaObject.CHARSET);
        GRAPHIC_IRV = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("ASCII_FULL", false,
                SchemaObject.CHARSET);
        ASCII_FULL = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("ISO8BIT", false,
                SchemaObject.CHARSET);
        ISO8BIT = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("UTF32", false,
                SchemaObject.CHARSET);
        UTF32 = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("UTF16", false,
                SchemaObject.CHARSET);
        UTF16 = new Charset(name);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("UTF8", false,
                SchemaObject.CHARSET);
        UTF8 = new Charset(name);
        /*
         * Foundattion 4.2.1
         * Character sets defined by standards or by SQL-implementations reside
         * in the Information Schema (named INFORMATION_SCHEMA) in each catalog,
         * as do collations defined by standards and collations,
         * transliterations, and transcodings defined by SQL implementations.
         */
    }

    public static final Type CARDINAL_NUMBER;
    public static final Type YES_OR_NO;
    public static final Type CHARACTER_DATA;
    public static final Type SQL_IDENTIFIER;
    public static final Type TIME_STAMP;
    public static final Type SQL_VARCHAR;

    static {
        HsqlName name;

        name = HsqlNameManager.newInfoSchemaObjectName("CARDINAL_NUMBER",
                false, SchemaObject.DOMAIN);
        CARDINAL_NUMBER = new NumberType(Types.SQL_BIGINT, 0, 0);
        CARDINAL_NUMBER.userTypeModifier = new UserTypeModifier(name,
                SchemaObject.DOMAIN, CARDINAL_NUMBER);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("YES_OR_NO", false,
                SchemaObject.DOMAIN);
        YES_OR_NO = new CharacterType(Types.SQL_VARCHAR, 3);
        YES_OR_NO.userTypeModifier = new UserTypeModifier(name,
                SchemaObject.DOMAIN, YES_OR_NO);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("CHARACTER_DATA",
                false, SchemaObject.DOMAIN);
        CHARACTER_DATA = new CharacterType(Types.SQL_VARCHAR, (1 << 16));
        CHARACTER_DATA.userTypeModifier = new UserTypeModifier(name,
                SchemaObject.DOMAIN, CHARACTER_DATA);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("SQL_IDENTIFIER",
                false, SchemaObject.DOMAIN);
        SQL_IDENTIFIER = new CharacterType(Types.SQL_VARCHAR, 128);
        SQL_IDENTIFIER.userTypeModifier = new UserTypeModifier(name,
                SchemaObject.DOMAIN, SQL_IDENTIFIER);

        //
        name = HsqlNameManager.newInfoSchemaObjectName("TIME_STAMP", false,
                SchemaObject.DOMAIN);
        TIME_STAMP = new DateTimeType(Types.SQL_TIMESTAMP,
                                      Types.SQL_TIMESTAMP, 6);
        TIME_STAMP.userTypeModifier = new UserTypeModifier(name,
                SchemaObject.DOMAIN, TIME_STAMP);

        //
        SQL_VARCHAR = Type.SQL_VARCHAR;
    }
}
