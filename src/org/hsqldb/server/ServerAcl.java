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


package org.hsqldb.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * A list of ACL permit and deny entries with a permitAccess method
 * which tells whether candidate addresses are permitted or denied
 * by this ACL list.
 **/
public class ServerAcl {

    static private class AclEntry {

        public long addr;
        public long mask;    // These are the bits in candidate which must match

        // addr
        public boolean allow;

        public AclEntry(long addr, long mask,
                        boolean allow) throws AclFormatException {

            this.addr  = addr;
            this.mask  = mask;
            this.allow = allow;

            validateMask();
        }

        public String toString() {
            return "ADDR " + longToDottedQuad(addr) + ", mask "
                   + longToDottedQuad(mask) + ", allow? " + allow;
        }

        public boolean covers(long candidate) {
            return addr == (mask & candidate);
        }

        public void validateMask() throws AclFormatException {

            if ((addr & ~mask) != 0) {
                throw new AclFormatException("The network address '"
                                             + longToDottedQuad(addr)
                                             + "' is too specific for mask '"
                                             + longToDottedQuad(mask) + "'");
            }
        }
    }

    static public class AclFormatException extends Exception {

        public AclFormatException(String s) {
            super(s);
        }
    }

    private PrintWriter pw = null;

    public void setPrintWriter(PrintWriter pw) {
        this.pw = pw;
    }

    public String toString() {

        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < aclEntries.size(); i++) {
            if (sb.length() > 0) {
                sb.append('\n');
            }

            sb.append("Entry " + (i + 1) + ": " + aclEntries.get(i));
        }

        return sb.toString();
    }

    private List            aclEntries = new ArrayList();
    static private AclEntry PROHIBIT_ALL;

    static {
        try {
            PROHIBIT_ALL = new AclEntry(0l, 0l, false);
        } catch (AclFormatException afe) {
            throw new RuntimeException(
                "Unexpected problem in static initializer", afe);
        }
    }

    /**
     * Utility method that allows interactive testing of individal
     * ACL records, as well as the net effect of the ACL record list.
     */
    public static void main(String[] sa)
    throws AclFormatException, IOException {

        if (sa.length > 1) {
            throw new RuntimeException(
                "SYNTAX: java org.hsqldb.ServerAcl filename.txt");
        }

        ServerAcl serverAcl = new ServerAcl(new File((sa.length == 0)
            ? "acl.txt"
            : sa[0]));

        serverAcl.setPrintWriter(new PrintWriter(System.out));
        System.out.println(serverAcl.toString());

        BufferedReader br =
            new BufferedReader(new InputStreamReader(System.in));

        System.out.println("Enter hostnames or IP addresses to be tested "
                           + "(one per line).");

        String s, addrString;
        long   addr;

        while ((s = br.readLine()) != null) {
            try {
                s = s.trim();

                if (s.length() < 1) {
                    continue;
                }

                System.out.println(
                    Boolean.toString(
                        (Character.isDigit(s.charAt(0))
                         ? serverAcl.permitAccess(s)
                         : serverAcl.permitAccess(
                             InetAddress.getByName(s)))));
            } catch (IOException ioe) {
                System.err.println(ioe.getMessage());
            }
        }
    }

    public boolean permitAccess(InetAddress inetAddr) {
        return permitAccess(inetAddrToLong(inetAddr));
    }

    public boolean permitAccess(String s) {
        return permitAccess(dottedQuadToLong(s));
    }

    public boolean permitAccess(long addr) {

        for (int i = 0; i < aclEntries.size(); i++) {
            if (((AclEntry) aclEntries.get(i)).covers(addr)) {
                AclEntry hit = (AclEntry) aclEntries.get(i);

                println("Addr '" + longToDottedQuad(addr) + "' "
                        + (hit.allow ? "permitted"
                                     : "denied") + " by rule " + (i + 1)
                                                 + ": " + hit);

                return hit.allow;
            }
        }

        throw new RuntimeException("No rule covers address '"
                                   + longToDottedQuad(addr) + "'");
    }

    private void println(String s) {

        if (pw == null) {
            return;
        }

        pw.println(s);
        pw.flush();
    }

    static private class InternalException extends Exception {}

    public ServerAcl(File aclFile) throws IOException, AclFormatException {

        if (!aclFile.exists()) {
            throw new IOException("File '" + aclFile.getAbsolutePath()
                                  + "' is not present");
        }

        if (!aclFile.isFile()) {
            throw new IOException("'" + aclFile.getAbsolutePath()
                                  + "' is not a regular file");
        }

        if (!aclFile.canRead()) {
            throw new IOException("'" + aclFile.getAbsolutePath()
                                  + "' is not accessible");
        }

        String          line;
        String          ruleTypeString;
        StringTokenizer toker;
        String          addrString;
        int             slashIndex;
        int             linenum = 0;
        long            mask;
        long            addr;
        String          maskString;
        boolean         allow;
        BufferedReader  br = new BufferedReader(new FileReader(aclFile));

        try {
            while ((line = br.readLine()) != null) {
                linenum++;

                line = line.trim();

                if (line.length() < 1) {
                    continue;
                }

                if (line.charAt(0) == '#') {
                    continue;
                }

                toker = new StringTokenizer(line);

                try {
                    if (toker.countTokens() != 2) {
                        throw new InternalException();
                    }

                    ruleTypeString = toker.nextToken();
                    addrString     = toker.nextToken();
                    slashIndex     = addrString.indexOf('/');

                    if (slashIndex < 0) {
                        mask = bitsToMask(32);
                        addr = dottedQuadToLong(addrString);
                    } else {
                        maskString = addrString.substring(slashIndex + 1);
                        mask = ((maskString.indexOf('.') > -1)
                                ? dottedQuadToLong(maskString)
                                : bitsToMask(Integer.parseInt(maskString)));
                        addr = dottedQuadToLong(addrString.substring(0,
                                slashIndex));
                    }

                    if (ruleTypeString.equalsIgnoreCase("allow")) {
                        allow = true;
                    } else if (ruleTypeString.equalsIgnoreCase("permit")) {
                        allow = true;
                    } else if (ruleTypeString.equalsIgnoreCase("prohibit")) {
                        allow = false;
                    } else if (ruleTypeString.equalsIgnoreCase("deny")) {
                        allow = false;
                    } else {
                        throw new InternalException();
                    }
                } catch (NumberFormatException nfe) {
                    throw new AclFormatException("Syntax error at ACL file '"
                                                 + aclFile.getAbsolutePath()
                                                 + "', line " + linenum);
                } catch (InternalException ie) {
                    throw new AclFormatException("Syntax error at ACL file '"
                                                 + aclFile.getAbsolutePath()
                                                 + "', line " + linenum);
                }

                try {
                    aclEntries.add(new AclEntry(addr, mask, allow));
                } catch (AclFormatException afe) {
                    throw new AclFormatException("Syntax error at ACL file '"
                                                 + aclFile.getAbsolutePath()
                                                 + "', line " + linenum
                                                 + ": " + afe.getMessage());
                }
            }
        } finally {
            br.close();
        }

        aclEntries.add(PROHIBIT_ALL);
    }

    static public long dottedQuadToLong(String s)
    throws NumberFormatException {

        long addr    = 0l;
        int  prevDot = -1;
        int  nextDot;

        for (int i = 0; i < 4; i++) {
            nextDot = s.indexOf('.', prevDot + 1);

            if (nextDot == s.length() - 1) {
                throw new NumberFormatException();
            }

            if (nextDot < 0) {
                if (i != 3) {
                    throw new NumberFormatException();
                }

                nextDot = s.length();
            }

            addr    *= 256;
            addr    += Long.parseLong(s.substring(prevDot + 1, nextDot));
            prevDot = nextDot;
        }

        return addr;
    }

    static public long bitsToMask(int bits) {

        long mask = 0l;

        for (int i = 0; i < 32; i++) {
            mask <<= 1;

            if (i < bits) {
                mask += 1;
            }
        }

        return mask;
    }

    static public String longToDottedQuad(long addr) {

        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < 4; i++) {
            if (sb.length() > 0) {
                sb.insert(0, '.');
            }

            sb.insert(0, Long.toString(addr & 255l));

            addr >>= 8;
        }

        return sb.toString();
    }

    static public long inetAddrToLong(InetAddress inetAddr)
    throws NumberFormatException {

        byte[] ba = inetAddr.getAddress();

        if (ba.length != 4) {
            throw new NumberFormatException("Address not an ipv4 addr: "
                                            + inetAddr);
        }

        long addr = 0l;

        for (int i = 0; i < ba.length; i++) {
            addr <<= 8;
            addr += ((ba[i] < 0) ? (256 + ba[i])
                                 : ba[i]);
        }

        return addr;
    }
}
