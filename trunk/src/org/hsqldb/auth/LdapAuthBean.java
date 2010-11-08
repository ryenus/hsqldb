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


package org.hsqldb.auth;

import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Hashtable;
import java.util.Properties;
import javax.naming.AuthenticationException;
import javax.naming.NamingException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.SearchResult;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.Attributes;
import javax.naming.directory.Attribute;
import javax.naming.ldap.StartTlsRequest;
import javax.naming.ldap.StartTlsResponse;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.ExtendedRequest;
import javax.naming.ldap.ExtendedResponse;
import org.hsqldb.lib.FrameworkLogger;

/**
 * Authenticates to a HyperSQL catalog according to entries in a LDAP
 * database.
 * If using LDAP StartTLS and your server has a certificate not trusted by
 * default by your JRE, then set system property 'javax.net.ssl.trustStore' to
 * the path to a trust store containing the cert (as well as any other certs
 * that your app needs for other purposes).
 * <P>
 * This class with authenticate login attempts against LDAP entries with RDN of
 * the HyperSQL account name (the precise attribute name defaults to 'uid', but
 * you may change that).
 * </P> <P>
 * This class purposefully does not support LDAPS, because LDAPS is deprecated
 * in favor of StartTLS, which we do support.
 * </P> <P>
 * This class does not support SASL/External authentication, because the work
 * involved with securely obtaining user-specific certs would be more complex
 * than everything else here combined.
 * Another AuthFunctionBean would have to be written if SASL/External is needed.
 * </P> <P>
 * To use instances of this class, you must use at least the methods
 * setLdapHost, setParentDn, and initialize.
 * </P> <P>
 * For a user to be given HyperSQL catalog access, that user must have a role
 * (matching the roleSchemaValue pattern here if any).
 * If what is wanted is to grant access but no role, then assign a dummy role
 * value, because HyperSQL will ignore unknown roles.
 * </P>
 *
 * @see AuthFunctionBean
 * @see #setLdapHost(String)
 * @see #setParentDn(String)
 * @see #initialize()
 */
public class LdapAuthBean implements AuthFunctionBean {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(LdapAuthBean.class);

    private Integer ldapPort;
    private String ldapHost, principalTemplate, saslRealm, parentDn;
    private Pattern roleSchemaValuePattern;
    private String initialContextFactory = "com.sun.jndi.ldap.LdapCtxFactory";
    private boolean tls;  // This is for StartTLS, not tunneled TLS/LDAPS.
                  // Variable named just "tls" only for brevity.
    private String mechanism = "SIMPLE";
    private String rdnAttribute = "uid";
    private boolean initialized;
    private String rolesSchemaAttribute = "memberof";

    /**
     * If this is set, then the entire (brief) transaction with the LDAP server
     * will be encrypted.
     */
    public void setStartTls(boolean isTls) {
        this.tls = isTls;
    }

    public LdapAuthBean() {
        // Intentionally empty
    }

    public void setLdapPort(int ldapPort) {
        this.ldapPort = Integer.valueOf(ldapPort);
    }

    /**
     * @throws IllegalStateException if any required setting has not been set.
     */
    public void init() {
        if (ldapHost == null) {
            throw new IllegalStateException(
                    "Required property 'ldapHost' not set");
        }
        if (parentDn == null) {
            throw new IllegalStateException(
                    "Required property 'parentDn' not set");
        }
        if (initialContextFactory == null) {
            throw new IllegalStateException(
                    "Required property 'initialContextFactory' not set");
        }
        if (mechanism == null) {
            throw new IllegalStateException(
                    "Required property 'mechanism' not set");
        }
        if (rdnAttribute == null) {
            throw new IllegalStateException(
                    "Required property 'rdnAttribute' not set");
        }
        if (rolesSchemaAttribute == null) {
            throw new IllegalStateException(
                    "Required property 'rolesSchemaAttribute' not set");
        }
        initialized = true;
    }

    /**
     * Assign a pattern to both detect desired values, and to map from a single
     * value of "rolesSchemaAttribute"s to a HyperSQL role or schema string.
     * If your rolesSchemaAttribute holds only the String values precisely as
     * HyperSQL needs them, then don't use this method at all and all matching
     * attribute values will be passed directly.
     * </P><P>
     * These are two distinct and important purposes for the specified Pattern.
     * <OL>
     *   <LI>
     *      Values that do not successfully match the pattern will be ignored.
     *   <LI>
     *      Optionally uses parentheses to specify a single capture group
     *      (if you use parentheses to specify more than one matching group, we
     *      will only capture for the first).
     *      What is captured by this group is exactly the role or schema that
     *      HyperSQL will attempt to assign.
     *      If no capture parens are given then the Pattern is only used for the
     *      acceptance decision, and the LDAP-provided value will be returned
     *      verbatim.
     * <P>
     * Together, these two features work great to extract just the needed role
     * and schema names from 'memberof' DNs, and will have no problem if you
     * also use 'memberof' for unrelated purposes.
     * </P><P>
     * N.b. this Pattern will be used for the matches() operation, therefore it
     * must match the entire candidate value strings (this is different than
     * the find operation which does not need to satisfy the entire candidate
     * value).
     * </P><P>Example1 :<CODE><PRE>
     *     cn=([^,]+),ou=dbRole,dc=admc,dc=com
     * </PRE></CODE>
     *     will extract the CN value from matching attribute values.
     * </P><P>Example1 :<CODE><PRE>
     *     cn=[^,]+,ou=dbRole,dc=admc,dc=com
     * </PRE></CODE>
     *     will return the entire <CODE>cn...com</CODE> string for matching
     *     attribute values.
     * </P>
     *
     * @see Matcher#matches()
     */
    public void setRoleSchemaValuePattern(Pattern roleSchemaValuePattern) {
        this.roleSchemaValuePattern = roleSchemaValuePattern;
    }

    /**
     * String wrapper for method setRoleSchemaValuePattern(Pattern)
     *
     * Use the (x?) Pattern constructs to set options.
     *
     * @throws java.util.regex.PatternSyntaxException
     * @see #setRoleSchemaValuePattern(Pattern)
     */
    public void setRoleSchemaValuePatternString(String patternString) {
        setRoleSchemaValuePattern(Pattern.compile(patternString));
    }

    /**
     * Defaults to "SIMPLE".
     * @param mechanism.  Either 'SIMPLE' (the default) for LDAP Simple, or
     *                    one of the LDAP SASL mechamisms, such as 'DIGEST-MD5'.
     */
    public void setSecurityMechanism(String mechanism) {
        this.mechanism = mechanism;
    }

    /**
     * Do not specify URL scheme ("ldap:") because that is implied.
     * (Since we purposefully don't support LDAPS, there would be no reason to
     * change that).
     * <P>
     * If using StartTLS, then this host name must match the cn of the LDAP
     * server's certificate.
     * </P>
     */
    public void setLdapHost(String ldapHost) {
        this.ldapHost = ldapHost;
    }

    /**
     * A template String containing place-holder token '${username}'.
     * All occurrences of '${username}' (without the quotes) will be translated
     * to the username that authentication is being attempted with.
     * <P>
     * If you supply a principalTemplate that does not contain '${username}',
     * then authentication will be user-independent.
     * </P> <P>
     * It is common to authenticate to LDAP servers with the DN of the user's
     * LDAP entry.  In this situation, set principalTemplate to
     * <CODE>&lt;RDN_ATTR=&gt;${username},&lt;PARENT_DN&gt;</CODE>.
     * For example if you use parentDn of
     * <CODE>"ou=people,dc=admc,dc=com"</CODE> and rdnAttribute of
     * <CODE>uid</CODE>, then you would set <CODE><PRE>
     *     "uid=${username},ou=people,dc=admc,dc=com"
     * </PRE></CODE>
     * </P> <P>
     * By default the user name will be passed exactly as it is, so don't use
     * this setter if that is what you want.  (This works great for OpenLDAP
     * with DIGEST-MD5 SASL, for example).
     * </P>
     */
    public void setPrincipalTemplate(String principalTemplate) {
        this.principalTemplate = principalTemplate;
    }

    /**
     * Most users should not call this, and will get the default of
     * "com.sun.jndi.ldap.LdapCtxFactory".
     * Use this method if you prefer to use a context factory provided by your
     * framework or container, for example, or if you are using a non-Sun JRE.
     */
    public void setInitialContextFactory(String initialContextFactory) {
        this.initialContextFactory = initialContextFactory;
    }

    /**
     * Some LDAP servers using a SASL mechanism require a realm to be specified,
     * and some mechanisms allow a realm to be specified if you wish to use that
     * feature.
     * By default no realm will be sent to the LDAP server.
     * <P>
     * Don't use this setter if you are not setting a SASL mechanism.
     * </P>
     */
    public void setSaslRealm(String saslRealm) {
        this.saslRealm = saslRealm;
    }

    /**
     * Set DN which is parent of the user DNs.
     * E.g.  "ou=people,dc=admc,dc=com"
     */
    public void setParentDn(String parentDn) {
        this.parentDn = parentDn;
    }

    /**
     * rdnAttribute must hold the user name exactly as the HyperSQL login will
     * be made with.
     * <P>
     * This is the RDN relateive to the Parent DN specified with setParentDN.
     * Defaults to 'uid'.
     * </P>
     *
     * @see #setParentDn(String)
     */
    public void setRdnAttribute(String rdnAttribute) {
        this.rdnAttribute = rdnAttribute;
    }

    /**
     * Set the attribute name of the RDN + parentDn entries in which is stored
     * the list of roles and optional schema for the authenticating user.
     * <P>
     * Defaults to 'memberof', so you can use the nice <i>reverse group
     * membership</i> feature of LDAP.
     * </P>
     */
    public void setRolesSchemaAttribute(String attribute) {
        rolesSchemaAttribute = attribute;
    }

    public static class DenyException extends Exception {
        // Intentionally empty
    }

    /**
     * @see AuthFunctionBean#authenticate(String, password)
     */
    public String[] authenticate(String userName, String password)
            throws DenyException {
        if (!initialized) {
            throw new IllegalStateException(
                "You must invoke the 'init' method to initialize the "
                + LdapAuthBean.class.getName() + " instance.");
        }
        Hashtable env = new Hashtable(5, 0.75f);
        env.put(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);
        env.put(Context.PROVIDER_URL, "ldap://" + ldapHost
                + ((ldapPort == null) ? "" : (":" + ldapPort)));
        StartTlsResponse tlsResponse = null;
        LdapContext ctx = null;
        List<String> returns = new ArrayList<String>();

        try {
            ctx = new InitialLdapContext(env, null);

            if (tls) {
                // Requesting to start TLS on an LDAP association
                tlsResponse = (StartTlsResponse) ctx.extendedOperation(
                        new StartTlsRequest());

                // Starting TLS
                tlsResponse.negotiate();
            }

            // A TLS/SSL secure channel has been established if you reach here.
          
            // Assertion of client's authorization Identity -- Explicit way
            ctx.addToEnvironment(Context.SECURITY_AUTHENTICATION, mechanism);
            ctx.addToEnvironment(Context.SECURITY_PRINCIPAL,
                  ((principalTemplate == null)
                  ? userName
                  : principalTemplate.replace("${username}", userName)));
            ctx.addToEnvironment(Context.SECURITY_CREDENTIALS, password);
            if (saslRealm != null) {
                env.put("java.naming.security.sasl.realm", saslRealm);
            }
          
            // The Context.SECURITY_* authorizations are only applied when the
            // following statement executes.  (Or any other remote operations done
            // while the TLS connection is still open).
            NamingEnumeration<SearchResult> sRess = null;
            try {
                sRess = ctx.search(parentDn,
                        new BasicAttributes(rdnAttribute, userName),
                        new String[] { rolesSchemaAttribute });
            } catch (AuthenticationException ae) {
                throw new DenyException();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (!sRess.hasMore()) {
                throw new DenyException();
            }
            SearchResult sRes = sRess.next();
            if (sRess.hasMore()) {
                throw new RuntimeException("> 1 result");
            }
            Attributes attrs = sRes.getAttributes();
            if (attrs.size() == 0) {
                // If LDAP wants a user to have access, it must return a
                // role matching the roleSchemaValuePattern.
                // See Class Javadocs.
                throw new DenyException();
            }
            if (attrs.size() != 1) {
                throw new RuntimeException("Wrong # of attrs: " + attrs.size());
            }
            Attribute attribute =  attrs.get(rolesSchemaAttribute);
            int valCount = attribute.size();
            //System.err.println("#" + valCount);
            Matcher matcher;
            for (int i = 0; i < valCount; i++) {
                if (attribute.get(i) == null) {
                    throw new RuntimeException("Attr value #" + i + " is null");
                }
                if (!(attribute.get(i) instanceof String)) {
                    throw new RuntimeException("Attr value #" + i
                            + " not a String: "
                            + attribute.get(i).getClass().getName());
                }
                if (roleSchemaValuePattern == null) {
                    returns.add((String) attribute.get(i));
                } else {
                    matcher = roleSchemaValuePattern.matcher(
                            (String) attribute.get(i));
                    if (matcher.matches()) {
                        returns.add((matcher.groupCount() > 0)
                                ? matcher.group(1)
                                : (String) attribute.get(i));
                    }
                }
            }
        } catch (DenyException de) {
            // This throws a non-runtime Exception, which is handled as an
            // access denial instead of a system problem.
            throw de;
        } catch (RuntimeException re) {
            throw re;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (NamingException ne) {
            throw new RuntimeException(ne);
        } finally {
            if (tlsResponse != null) try {
                tlsResponse.close();
            } catch (IOException ioe) {
                logger.error("Failed to close TLS Response", ioe);
            }
            if (ctx != null) try {
                ctx.close();
            } catch (NamingException ne) {
                logger.error("Failed to close LDAP Context", ne);
            }
        }
        if (returns.size() < 1) {
            throw new DenyException();
        }
        return returns.toArray(new String[0]);
    }

    /**
     * Run this method to try and test configuration settings for LdapAuthBeans,
     * or to troubleshoot.
     * It purposefully does not test the Java Function or the JDBC layer at all.
     * This program will attempt to retrieve and display the schema/roles list
     * for the specified user and with the specified password from the LDAP
     * server according to the specified properties.
     * <P>
     * Passwords typed on the command line are inherently not secure, so only
     * use this program when the computer it is run on is secured and where
     * your command line may not be observed, directly or indirectly, by others.
     * </P> <P>
     * Set the properties in a properties file to match your LDAP security and
     * Directory Information Tree structure and use this program to check
     * everything between the LdapAuthBean and your LDAP server.
     * You then know the exact settings to use for an LdapAuthBean that you can
     * plug into AuthBeanMultiplexer.
     * </P> <P>
     * Run with no arguments to see required syntax.
     * </P> <P>
     *   The property file may contain any of the following properties, which
     *   exactly match the corresponding setter methods in this class.
     *   <UL>
     *     <LI>trustStore.  This is the only property without a corresponding
     *         setter method.  Setting this property has the same effect as
     *         setting Java system property 
     *         <CODE>'javax.net.ssl.trustStore'<CODE>.
     *     <LI>startTls.  Takes a boolean value according to
     *         method java.util.Boolean.parseBoolean.
     *     <LI>roleSchemaValuePattern.  Correponds to method
     *         setRoleSchemaValuePatternString
     *     <LI>ldapPort
     *     <LI>securityMechanism
     *     <LI>ldapHost
     *     <LI>principalTemplate
     *     <LI>initialContextFactory
     *     <LI>saslRealm
     *     <LI>parentDn
     *     <LI>rdnAttribute
     *     <LI>rolesSchemaAttribute
     *   </UL>
     *   Tokens like ${this} will not be expanded to system property values,
     *   and your bean will get the values exactly as you type them in.
     * </P> <P>
     * The file sample/ldap-exerciser.properties in the HyperSQL distribution
     * may be used as a template or example.
     * </P>
     */
    public static void main(String[] sa) throws IOException {
        if (sa.length != 3) {
            throw new IllegalArgumentException(
                    "SYNTAX:  java " + AuthBeanMultiplexer.class.getName()
                    + " path/to/file.properties <USERNAME> <PASSWORD>");
        }
        File file = new File(sa[0]);
        if (!file.isFile()) {
            throw new IllegalArgumentException(
                    "Not a file: " + file.getAbsolutePath());
        }
        Properties p = new Properties();
        p.load(new FileInputStream(file));
        String trustStore = p.getProperty("trustStore");
        String startTlsString = p.getProperty("startTls");
        String ldapPortString = p.getProperty("ldapPort");
        String roleSchemaValuePatternString =
                p.getProperty("roleSchemaValuePattern");
        String securityMechanism = p.getProperty("securityMechanism");
        String ldapHost = p.getProperty("ldapHost");
        String principalTemplate = p.getProperty("principalTemplate");
        String initialContextFactory = p.getProperty("initialContextFactory");
        String saslRealm = p.getProperty("saslRealm");
        String parentDn = p.getProperty("parentDn");
        String rdnAttribute = p.getProperty("rdnAttribute");
        String rolesSchemaAttribute = p.getProperty("rolesSchemaAttribute");
        if (trustStore != null) {
            if (!(new File(trustStore)).isFile()) {
                throw new IllegalArgumentException(
                        "Specified trust store is not a file: " + trustStore);
            }
            System.setProperty("javax.net.ssl.trustStore", trustStore);
        }
        LdapAuthBean bean = new LdapAuthBean();
        if (startTlsString != null) {
            bean.setStartTls(Boolean.parseBoolean(startTlsString));
        }
        if (ldapPortString != null) {
            bean.setLdapPort(Integer.parseInt(ldapPortString));
        }
        if (roleSchemaValuePatternString != null) {
            bean.setRoleSchemaValuePatternString(roleSchemaValuePatternString);
        }
        if (securityMechanism != null) {
            bean.setSecurityMechanism(securityMechanism);
        }
        if (ldapHost != null) {
            bean.setLdapHost(ldapHost);
        }
        if (principalTemplate != null) {
            bean.setPrincipalTemplate(principalTemplate);
        }
        if (initialContextFactory != null) {
            bean.setInitialContextFactory(initialContextFactory);
        }
        if (saslRealm != null) {
            bean.setSaslRealm(saslRealm);
        }
        if (parentDn != null) {
            bean.setParentDn(parentDn);
        }
        if (rdnAttribute != null) {
            bean.setRdnAttribute(rdnAttribute);
        }
        if (rolesSchemaAttribute != null) {
            bean.setRolesSchemaAttribute(rolesSchemaAttribute);
        }
        bean.init();
        String[] res = null;
        try {
            res = bean.authenticate(sa[1], sa[2]);
        } catch (DenyException de) {
            System.out.println("<DENIED>");
            return;
        }
        if (res == null) {
            System.out.println("<NULL>");
        } else {
            System.out.println(Integer.toString(res.length)
                    + " roles/schema: " + Arrays.toString(res));
        }
    }
}
