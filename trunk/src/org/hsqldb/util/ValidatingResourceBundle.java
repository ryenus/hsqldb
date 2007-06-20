/* Copyright (c) 2007, The HSQL Development Group
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
 *
 * $Id$
 */


package org.hsqldb.util;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Enumeration;

/**
 * Purpose of this class is to wrap a RefCapablePropertyResourceBundle to
 *  reliably detect any possible use of a missing property key as soon as
 *  this class is clinitted.
 * The reason for this is to allow us developers to detect all such errors
 *  before end-users ever use this class.
 *
 * See SqltoolRB for an example implementation of this abstract class.
 */
abstract public class ValidatingResourceBundle {
    protected boolean validated = false;
    abstract protected Map getKeyIdToString();

    public static final int THROW_BEHAVIOR =
            RefCapablePropertyResourceBundle.THROW_BEHAVIOR;
    public static final int EMPTYSTRING_BEHAVIOR =
            RefCapablePropertyResourceBundle.EMPTYSTRING_BEHAVIOR;
    public static final int NOOP_BEHAVIOR =
            RefCapablePropertyResourceBundle.NOOP_BEHAVIOR;
    /* Three constants above are only so caller doesn't need to know
     * details of RefCapablePropertyResourceBundle (and they won't need
     * to code that God-awfully-long class name). */

    protected RefCapablePropertyResourceBundle wrappedRCPRB;

    protected ValidatingResourceBundle(String baseName) {
        wrappedRCPRB = RefCapablePropertyResourceBundle.getBundle(baseName,
                getClass().getClassLoader());
    }

    // The following methods are a passthru wrappers for the wrapped RCPRB.

    /** @see RefCapablePropertyResourceBundle#getString(String) */
    public String getString(int id) {
        return wrappedRCPRB.getString((String) getKeyIdToString().get(id));
    }

    /** @see RefCapablePropertyResourceBundle#getString(String, String[]) */
    public String getString(int id, String[] sa) {
        return wrappedRCPRB.getString((String) getKeyIdToString().get(id), sa,
                missingPosValueBehavior);
    }

    /** @see RefCapablePropertyResourceBundle#getExpandedString(String) */
    public String getExpandedString(int id) {
        return wrappedRCPRB.getExpandedString(
                (String) getKeyIdToString().get(id),
                missingPropertyBehavior);
    }

    /** @see RefCapablePropertyResourceBundle#getExpandedString(String, String[]) */
    public String getExpandedString(int id, String[] sa) {
        return wrappedRCPRB.getExpandedString(
                (String) getKeyIdToString().get(id), sa,
                missingPropertyBehavior, missingPosValueBehavior);
    }

    private int missingPropertyBehavior = THROW_BEHAVIOR;
    private int missingPosValueBehavior = THROW_BEHAVIOR;

    /**
     * Set behavior for get*String*() method when a referred-to
     * System Property is not set.  Set to one of
     * <UL>
     *  <LI>RefCapablePropertyResourceBunele.THROW_BEHAVIOR
     *  <LI>RefCapablePropertyResourceBunele.EMPTYSTRING_BEHAVIOR
     *  <LI>RefCapablePropertyResourceBunele.NOOP_BEHAVIOR
     * </UL>
     * The first value is the default.
     */
    public void setMissingPropertyBehavior(int missingPropertyBehavior) {
        this.missingPropertyBehavior = missingPropertyBehavior;
    }
    /**
     * Set behavior for get*String(String, String[]) method when a
     * positional index (like %{4}) is used but no subs value was given for
     * that index.  Set to one of
     * <UL>
     *  <LI>RefCapablePropertyResourceBunele.THROW_BEHAVIOR
     *  <LI>RefCapablePropertyResourceBunele.EMPTYSTRING_BEHAVIOR
     *  <LI>RefCapablePropertyResourceBunele.NOOP_BEHAVIOR
     * </UL>
     * The first value is the default.
     */
    public void setMissingPosValueBehavior(int missingPosValueBehavior) {
        this.missingPosValueBehavior = missingPosValueBehavior;
    }

    public int getMissingPropertyBehavior() {
        return missingPropertyBehavior;
    }
    public int getMissingPosValueBehavior() {
        return missingPosValueBehavior;
    }

    /**
     * Returns the number of defined (usable) keys.
     */
    public int getSize() {
        if (!validated)
            throw new RuntimeException(
                    "Method SqltoolRB.getSize() may only be called after "
                    + "calling SqltoolRB.validate()");
        return getKeyIdToString().size();
    }

    public void validate() {
        String val;
        if (validated) return;
        validated = true;
        Set allIdStrings = new HashSet(getKeyIdToString().values());
        Enumeration allKeys = wrappedRCPRB.getKeys();
        while (allKeys.hasMoreElements()) {
            // We can't test positional parameters, but we can verify that
            // referenced files exist by reading the values.
            // Pretty inefficient, but this can be optimized when I have time.
            val = (String) allKeys.nextElement();
            wrappedRCPRB.getString(val);
            // Keep no reference to the returned String
            allIdStrings.remove(val);
        }
        if (allIdStrings.size() > 0)
            throw new RuntimeException(
                    "Resource Bundle pre-validation failed.  "
                    + "Following property key(s) not mapped.\n" + allIdStrings);
    }
}
