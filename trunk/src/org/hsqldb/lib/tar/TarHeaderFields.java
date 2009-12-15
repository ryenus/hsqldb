/* Copyright (c) 2001-2009, The HSQL Development Group
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

import java.util.HashMap;
import java.util.Map;

/**
 * Purely static structure defining our interface to the Tar Entry Header.
 *
 * The fields controlled here are fields for the individual tar file entries
 * in an archive.  There is no such thing as a Header Field at the top archive
 * level.  * <P>
 * We use header field names as they are specified in the FreeBSD man page for
 * tar in section 5 (Solaris and Linux have no such page in section 5).
 * Where we use a constant, the constant name is just the FreeBSD field name
 * capitalized.
 * Since a single field is known as either "linkflag" or "typeflag", we are
 * going with the UStar name typeflag for this field.
 * <P>
 * We purposefully define no variable for this list of fields, since
 * we DO NOT WANT TO access or change these values, due to application
 * goals or JVM limitations:<UL>
 *   <LI>gid
 *   <LI>uid
 *   <LI>linkname
 *   <LI>magic (UStar ID),
 *   <LI>magic version
 *   <LI>group name
 *   <LI>device major num
 *   <LI>device minor num
 * </UL>
 * Our application has no use for these, or Java has no ability to
 * work with them.
 * <P>
 * This class will be very elegant when refactored as an enum with enumMap(s)
 * and using generics with auto-boxing instead of the ugly and non-validating
 * casts.
 *
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 */
@SuppressWarnings("boxing")
public class TarHeaderFields {

    final static int NAME     = 1;
    final static int MODE     = 2;
    final static int UID      = 3;
    final static int GID      = 4;
    final static int SIZE     = 5;
    final static int MTIME    = 6;        // (File.lastModified()|*.getTime())/1000
    final static int CHECKSUM = 7;
    final static int TYPEFLAG = 8;

    // The remaining are from UStar format:
    final static int MAGIC  = 9;
    final static int UNAME  = 10;
    final static int GNAME  = 11;
    final static int PREFIX = 12;

    // Replace these contants with proper enum once we require Java 1.5.
    static Map<Integer, String> labels = new HashMap<Integer, String>();
    // String identifier

    // (this supplied automatically by enums)
    static Map<Integer, Integer> starts = new HashMap<Integer, Integer>();
    // Starting positions
    static Map<Integer, Integer> stops  = new HashMap<Integer, Integer>();

    // 1 PAST last position (in normal Java substring fashion).
    /* Note that (with one exception), there is always 1 byte
     * between a numeric field stop and the next start.  This is
     * because null byte must occupy the intervening position.
     * This is not true for non-numeric fields (which includes the
     * link-indicator/type-flag field, which is used as a code,
     * and is not necessarily numeric with UStar format).
     *
     * As a consequence, there may be NO DELIMITER after
     * non-numerics, which may occupy the entire field segment.
     *
     * Arg.  man page for "pax" says that both original and ustar
     * headers must be <= 100 chars. INCLUDING the trailing \0
     * character.  ???  GNU tar certainly does not honor this.
     */
    static {
        labels.put(NAME, "name");
        starts.put(NAME, 0);
        stops.put(NAME, 100);
        labels.put(MODE, "mode");
        starts.put(MODE, 100);
        stops.put(MODE, 107);
        labels.put(UID, "uid");
        starts.put(UID, 108);
        stops.put(UID, 115);
        labels.put(GID, "gid");
        starts.put(GID, 116);
        stops.put(GID, 123);
        labels.put(SIZE, "size");
        starts.put(SIZE, 124);
        stops.put(SIZE, 135);
        labels.put(MTIME, "mtime");
        starts.put(MTIME, 136);
        stops.put(MTIME, 147);
        labels.put(CHECKSUM, "checksum");          // Queer terminator.

        // Pax UStore does not follow spec and delimits this field like
        // any other numeric, skipping the space byte.
        starts.put(CHECKSUM, 148);    // Special fmt.
        stops.put(CHECKSUM, 156);     // Queer terminator.
        labels.put(TYPEFLAG, "typeflag");
        starts.put(TYPEFLAG, 156);    // 1-byte CODE

        // With current version, we are never doing anything with this
        // field.  In future, we will support x and/or g type here.
        stops.put(TYPEFLAG, 157);
        labels.put(MAGIC, "magic");

        // N.b. Gnu Tar does not honor this Stop.
        starts.put(MAGIC, 257);
        stops.put(MAGIC, 263);
        labels.put(UNAME, "uname");
        starts.put(UNAME, 265);
        stops.put(UNAME, 296);
        labels.put(GNAME, "gname");
        starts.put(GNAME, 297);
        stops.put(GNAME, 328);
        labels.put(PREFIX, "prefix");
        starts.put(PREFIX, 345);
        stops.put(PREFIX, 399);
    }

    // The getters below throw RuntimExceptions instead of
    // TarMalformatExceptions because these errors indicate a dev problem,
    // not some problem with a Header, or generating or reading a Header.
    static public int getStart(int field) {

        Integer iObject = starts.get(field);

        if (iObject == null) {
            throw new IllegalArgumentException(
                RB.singleton.getString(RB.UNEXPECTED_HEADER_KEY, field));
        }

        return iObject.intValue();
    }

    static public int getStop(int field) {

        Integer iObject = stops.get(field);

        if (iObject == null) {
            throw new IllegalArgumentException(
                RB.singleton.getString(RB.UNEXPECTED_HEADER_KEY, field));
        }

        return iObject.intValue();
    }

    static public String toString(int field) {

        String s = labels.get(field);

        if (s == null) {
            throw new IllegalArgumentException(
                RB.singleton.getString(RB.UNEXPECTED_HEADER_KEY, field));
        }

        return s;
    }
}
