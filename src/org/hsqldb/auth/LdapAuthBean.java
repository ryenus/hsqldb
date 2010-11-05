package org.hsqldb.auth;

/**
 *  Authenticates to a HyperSQL catalog according to entries in a LDAP
 *  database.
 *
 * @see AuthFunctionBean
 */

import java.util.*;
import javax.naming.*;
import javax.naming.directory.*;
import javax.naming.ldap.*;

public class LdapAuthBean implements AuthFunctionBean {
    private Integer ldapPort;
    private String ldapHost;

    public LdapAuthBean() {
        // Intentionally empty
    }

    public String[] authenticate(String userName, String password) {
return null;
    }
}

/*  WORKING LDAP CODE that does nearly everything that must be implemented by
 *  this class.
  public static void main (String[] args) {
    boolean isTls = true;
    boolean isDigestMd5 = true;

    System.setProperty("javax.net.ssl.trustStore", "/home/blaine/ca/cacert.store");
    
    Hashtable env = new Hashtable(5, 0.75f);
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.PROVIDER_URL, "ldap://beyla.admc.com:389");  
    StartTlsResponse tls = null;

    try{
      LdapContext ctx = new InitialLdapContext(env, null);

      if (isTls) {
      // Requesting to start TLS on an LDAP association
      ExtendedRequest tlsRequest = new StartTlsRequest();
      ExtendedResponse tlsResponse = ctx.extendedOperation(tlsRequest);
      
      // Starting TLS
      tls = (StartTlsResponse)tlsResponse;
      tls.negotiate();
      }

      // A TLS/SSL secure channel has been established if you reach here.
      
      // Assertion of client's authorization Identity -- Explicit way
      ctx.addToEnvironment(Context.SECURITY_AUTHENTICATION,
              isDigestMd5 ? "DIGEST-MD5" : "simple");
      ctx.addToEnvironment(Context.SECURITY_PRINCIPAL, isDigestMd5
              ? "straight" : "uid=straight,ou=people,dc=admc,dc=com");
      ctx.addToEnvironment(Context.SECURITY_CREDENTIALS, "pwd");
      // env.put("java.naming.security.sasl.realm", "JNDITutorial");
      
      // The Context.SECURITY_* authorizations are only applied when the
      // following statement executes.  (Or any other remote operations done
      // while the TLS connection is still open).
      //Attributes result = ctx.getAttributes("uid=straight,ou=people,dc=admc,dc=com");
      //System.out.println(result);
      NamingEnumeration<SearchResult> sRess = ctx.search(
              "ou=people,dc=admc,dc=com",
              new BasicAttributes("uid", "bren"),
              new String[] { "memberof"});
      if (!sRess.hasMore()) throw new Exception("No results");
      SearchResult sRes = sRess.next();
      if (sRess.hasMore()) throw new Exception("> 1 result");
      Attributes attrs = sRes.getAttributes();
      if (attrs.size() != 1)
          throw new Exception("Wrong # of attrs: " + attrs.size());
      Attribute attribute =  attrs.get("memberof");
      int valCount = attribute.size();
      System.err.println("#" + valCount);
      for (int i = 0; i < valCount; i++) {
          if (!(attribute.get(i) instanceof String))
              throw new Exception("Attr value #" + i + " not a String: "
                      + attribute.get(i).getClass().getName());
          System.err.println("=" + (String) attribute.get(i));
      }
            
      if (tls != null) tls.close();
                              
      // The TLS/SSL secure layer has been closed and all traffic down the road 
      // will be in clear text.
      
      // Non-privileged LDAP operations should be done here.
      ctx.close();
      */
