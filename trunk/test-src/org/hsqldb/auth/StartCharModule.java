/* Copyright (c) 2001-2011, The HSQL Development Group
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


package org.hsqldb.auth;

import java.security.Principal;
import javax.security.auth.spi.LoginModule;
import java.util.Map;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginException;
import javax.security.auth.Subject;
import org.hsqldb.lib.FrameworkLogger;

/**
 * A trivial sample JAAS Module that permits login access
 * as long the supplied user name and password both begin
 * with (case-sensitive) characters set with the relevant options.
 * Adds to the Subject credentials "RS:CHANGE_AUTHORIZATION" and "RS:ROLE1"
 * (both Strings), and principals with names "RS:ROLE2" and "RS:S1".
 *
 * This class is purposefully not secure and takes no pains to protect or clear
 * passwords from memory.
 */
public class StartCharModule implements LoginModule {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(StartCharModule.class);

    private String uName, password;
    private CallbackHandler handler;
    private Subject subject;
    private char nameStart;
    private char pwdStart;
    private boolean debug;

    public void initialize(Subject subject, CallbackHandler callbackHandler,
            Map<String,?> sharedState, Map<String,?> options) {
        this.subject = subject;
        handler = callbackHandler;
        if (options.containsKey("nameStart")
                && options.get("nameStart") instanceof String
                && ((String) options.get("nameStart")).length() == 1)
            nameStart = ((String) options.get("nameStart")).charAt(0);
        if (options.containsKey("pwdStart")
                && options.get("pwdStart") instanceof String
                && ((String) options.get("pwdStart")).length() == 1)
            pwdStart = ((String) options.get("pwdStart")).charAt(0);
        debug = options.containsKey("debug")
                && options.get("nameStart") instanceof String
                && Boolean.parseBoolean((String) options.get("debug"));
        if (debug)
            System.err.println("Initialized with chars "
                    + nameStart + " and " + pwdStart);
    }

    public boolean login() throws LoginException {
        if (handler == null) throw new LoginException("No CallbackHandler set");
        if (nameStart == '\0')
            throw new LoginException(
                    "Required 1-char option 'nameStart' not set");
        if (pwdStart == '\0')
            throw new LoginException(
                    "Required 1-char option 'pwdStart' not set");
        NameCallback nameCallback = new NameCallback("name");
        PasswordCallback pwdCallback = new PasswordCallback("pwd", true);
        try {
            handler.handle(new Callback[] { nameCallback, pwdCallback });
        } catch (Exception e) {
            if (e.getMessage() == null) {
                throw new LoginException();
            } else {
                throw new LoginException(e.getMessage());
            }
        }
        uName = nameCallback.getName();
        password = String.valueOf(pwdCallback.getPassword());
        return true;
    }

    public static class RolePrincipal implements Principal {
        private String roleName;
        public RolePrincipal(String roleName) {
            this.roleName = roleName;
        }
        public int hashCode() {
            return roleName.hashCode();
        }
        public String toString() {
            return roleName;
        }
        public String getName() {
            return roleName;
        }
        public boolean equals(Object other) {
            return (other instanceof RolePrincipal)
                    && ((RolePrincipal) other).toString().equals(roleName);
        }
    }

    public boolean commit() throws LoginException {
        if (uName.length() < 1 || password.length() < 1
                || uName.charAt(0) != nameStart
                || password.charAt(0) != pwdStart) {
            if (debug) System.err.println(
                (uName.length() < 1 || password.length() < 1)
                ? "Rejecting due to 0-length username or password"
                : ("Rejecting due to non-matching chars: " + uName.charAt(0)
                        + " and " + password.charAt(0)));
            return false;
        }
        subject.getPublicCredentials().add("RS:CHANGE_AUTHORIZATION");
        subject.getPublicCredentials().add("RS:ROLE1");
        subject.getPrincipals().add(new RolePrincipal("RS:S1"));
        subject.getPrincipals().add(new RolePrincipal("RS:ROLE2"));
        if (debug) System.err.println("Permitting");
        return true;
    }

    public boolean abort() throws LoginException {
        uName = password = null;
        return true;
    }

    public boolean logout() throws LoginException {
        uName = password = null;
        return true;
    }
}
