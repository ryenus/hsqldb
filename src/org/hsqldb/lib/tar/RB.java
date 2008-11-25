/* Copyright (c) 2001-2008, The HSQL Development Group
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


package org.hsqldb.lib.tar;

import java.util.Map;
import java.util.HashMap;
import org.hsqldb.util.ValidatingResourceBundle;
import org.hsqldb.util.RefCapablePropertyResourceBundle;
import org.hsqldb.util.ValidatingResourceBundle;

/* $Id$ */

/**
 * Resource Bundle for Tar classes
 *
 * Purpose of this class is to wrap a RefCapablePropertyResourceBundle to
 *  reliably detect any possible use of a missing property key as soon as
 *  this class is clinitted.
 * The reason for this is to allow us developers to detect all such errors
 *  before end-users ever use this class.
 *
 * IMPORTANT:  To add a new ResourceBundle element, add two new lines, one
 * like <PRE>
 *    static public final int NEWKEYID = keyCounter++;
 * </PRE> and one line <PRE>
 *      new Integer(KEY2), "key2",
 * </PRE>
 * Both should be inserted right after all of the other lines of the same type.
 * NEWKEYID is obviously a new constant which you will use in calling code
 * like RB.NEWKEYID.
 */
public class RB extends ValidatingResourceBundle {
    static private int keyCounter = 0;
    static public final int DBBACKUP_SYNTAX = keyCounter++;
    static public final int DBBACKUP_SYNTAXERR = keyCounter++;
    static public final int TARGENERATOR_SYNTAX = keyCounter++;
    static public final int PAD_BLOCK_WRITE = keyCounter++;
    static public final int CLEANUP_RMFAIL = keyCounter++;
    static public final int TARREADER_SYNTAX = keyCounter++;
    static public final int UNSUPPORTED_ENTRY_PRESENT = keyCounter++;
    static public final int BPR_WRITE = keyCounter++;
    static public final int STREAM_BUFFER_REPORT = keyCounter++;
    static public final int WRITE_QUEUE_REPORT = keyCounter++;

    private static Object[] memberKeyArray = new Object[] {
        /* With Java 5, can use auto-boxing and get rid of all of the
         * Integer instantiations below.*/
        new Integer(DBBACKUP_SYNTAX), "DbBackup.syntax",
        new Integer(DBBACKUP_SYNTAXERR), "DbBackup.syntaxerr",
        new Integer(TARGENERATOR_SYNTAX), "TarGenerator.syntax",
        new Integer(PAD_BLOCK_WRITE), "pad.block.write",
        new Integer(CLEANUP_RMFAIL), "cleanup.rmfail",
        new Integer(TARREADER_SYNTAX), "TarReader.syntax",
        new Integer(UNSUPPORTED_ENTRY_PRESENT), "unsupported.entry.present",
        new Integer(BPR_WRITE), "bpr.write",
        new Integer(STREAM_BUFFER_REPORT), "stream.buffer.report",
        new Integer(WRITE_QUEUE_REPORT), "write.queue.report",
    };

    private Map keyIdToString = new HashMap();

    protected Map getKeyIdToString() {
        return keyIdToString;
    }

    public RB() {
        super("org.hsqldb.lib.tar.rb");
        if (memberKeyArray == null)
            throw new RuntimeException("'static memberKeyArray not set");
        for (int i = 0; i < memberKeyArray.length; i += 2) {
            keyIdToString.put(memberKeyArray[i], memberKeyArray[i+1]);
        }
    }

    static {
        if (memberKeyArray == null)
            throw new RuntimeException("'static memberKeyArray not set");
        if (memberKeyArray.length % 2 != 0)
            throw new RuntimeException("memberKeyArray has an odd length");
        for (int i = 0; i < memberKeyArray.length; i += 2) {
            if (!(memberKeyArray[i] instanceof Integer))
                throw new RuntimeException("Element #" + i + " ("
                        + ((i - 1 < 0) ? "first item"
                            : ("after item \"" + memberKeyArray[i-1] + "\""))
                        + ") is a " + memberKeyArray[i].getClass().getName()
                        + ", not an Integer, in memberKeyArray in class "
                        + RB.class.getName());
            if (!(memberKeyArray[i+1] instanceof String))
                throw new RuntimeException("Element #" + (i+1) + " ("
                        + ((i - 2 < 0) ? "first item"
                            : ("after item \"" + memberKeyArray[i-1] + "\""))
                        + ") is a " + memberKeyArray[i+1].getClass().getName()
                        + ", not a String, in memberKeyArray in class "
                        + RB.class.getName());
            if (((Integer) memberKeyArray[i]).intValue() != i/2)
                throw new RuntimeException("Element #" +  i
                        + " is wrong constant for item " + memberKeyArray[i+1]
                        + " in memberKeyArray in class "
                        + RB.class.getName());
        }
    }

    /* IMPORTANT:  Leave the singleton instantiation at the end here!
     * Otherwise there will be a confusing tangle between clinitting and
     * singleton instantiation.  */
    static public RB singleton = new RB();
    static {
        singleton.validate();
    }
}
