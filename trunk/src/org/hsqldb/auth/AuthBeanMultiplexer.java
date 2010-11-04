package org.hsqldb.auth;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import org.hsqldb.jdbc.JDBCArrayBasic;
import org.hsqldb.types.Type;

/**
 * This class provides a method which can be used directly as a HyperSQL static
 * Java function method.
 * Manages a set of AuthTriggerBean implementations
 */
public class AuthBeanMultiplexer {
    /**
     * This sole constructor is purposefully private, so users or frameworks
     * that want to work with instances will be forced to use the singleton
     * instead of creating useless extra instance copies.
     */
    private AuthBeanMultiplexer() {
        // Intentionally empty
    }

    private static AuthBeanMultiplexer singleton = new AuthBeanMultiplexer();

    /**
     * @see #setAuthTriggerBeans(List)
     */
    private static Map<String, List<AuthTriggerBean>> beans =
            new HashMap<String, List<AuthTriggerBean>>();

    public static AuthBeanMultiplexer getSingleton() {
        return singleton;
    }

    /**
     * Clear the set of AuthTriggerBeans
     */
    public void clear() {
        AuthBeanMultiplexer.beans.clear();
    }

    /**
     * Primary purpose of this class is to manage this static map.
     * From dbNames to ordered-lists-of-AuthTriggerBeans.
     * This is not an "adder" function, but a "setter" function, so do not use
     * this to add to a partial set, but to assign the entire set.
     * <P>
     * The given entries are copied, to limit side-effects and concurrency
     * issues.
     * </P>
     */
    public void setAuthTriggerBeans(
            Map<String, List<AuthTriggerBean>> authTriggerBeanMap) {
        if (AuthBeanMultiplexer.beans.size() > 0)
            throw new IllegalStateException(
                    "Use setAuthTriggerBeans(Map) only when the set is empty");
        AuthBeanMultiplexer.beans.putAll(authTriggerBeanMap);
    }

    /**
     * This is not an "adder" function, but a "setter" function for the
     * specified dbName , so do not use this to add to a database's
     * TriggerBeans, but to assign the entire list for that database.
     * <P>
     * The given entries are copied, to limit side-effects and concurrency
     * issues.
     * </P> <P>
     * Use this method instead of setAuthorTriggerBean(String, AuthTriggerBean)
     * in order to set up multiple authenticators for a single database for
     * redundancy purposes.
     * </P>
     *
     * @see setAuthTriggerBeans(Map)
     * @see setAuthTriggerBean(String, AuthTriggerBean)
     */
    public void setAuthTriggerBeans(String dbName,
            List<AuthTriggerBean> authTriggerBeans) {
        List<AuthTriggerBean> dbsBeans = AuthBeanMultiplexer.beans.get(dbName);
        if (dbsBeans == null) {
            dbsBeans = new ArrayList<AuthTriggerBean>();
            AuthBeanMultiplexer.beans.put(dbName, dbsBeans);
        } else {
            if (dbsBeans.size() > 0)
                throw new IllegalStateException(
                        "Use setAuthTriggerBeans(String, List) only when the "
                        + "db's AuthTriggerBean list is empty");
        }
        dbsBeans.addAll(authTriggerBeans);
    }

    /**
     * This is not an "adder" function, but a "setter" function for the
     * specified dbName , so do not use this to add to a database's
     * TriggerBeans, but to assign ths single given AuthTriggerBean as the
     * specified database's authenticator.
     * <P>
     * To set up multiple authenticators for a single database for redundancy
     * purposes, use the method setAuthorTriggerBeans(String, List) instead.
     * </P>
     *
     * @see setAuthTriggerBeans(String, List)
     */
    public void setAuthTriggerBean(String dbName,
            AuthTriggerBean authTriggerBean) {
        setAuthTriggerBeans(dbName, Collections.singletonList(authTriggerBean));
    }

    /**
     * HyperSQL Java Function Method.
     * <P>
     * Registered AuthTriggerBeans matching the specified database and password
     * will be tried in order.
     * <OL>
     *   <li>If the AuthTriggerBean being tried throws a non-runtime Exception,
     *       then that RuntimeException is passed through (re-thrown), resulting
     *       in a SQLException for the authenticating application.
     *   <li>If the AuthTriggerBean being tried doesn't throw anything, then
     *       the return value is passed through (returned) and HyperSQL will
     *       allow access and set roles according to HyperSQL's authentication
     *       function contract.
     *   <LI>If the AuthTriggerBean being tried throws a RuntimeException, then
     *       the next AuthTriggerBean in turn will be tried.
     *       If all matching AuthTriggerBeans throw RuntimeExceptions, then the
     *       first RuntimeException that was thrown will be passed through
     *       (re-thrown), resulting in a SQLException for the authenticating
     *       application.
     *   <LI>If there are no AuthTriggerBeans registered for the specified
     *       dbName, then this method will throw an IllegalArgumentException,
     *       resulting in a SQLException for the authenticating application.
     * </OL>
     *
     * @See HyperSQL User Guide, System Management and Deployment Issues
     *         chapter, Authentication Settings subsection.
     * @throws IllegalArgumentException if no AuthTriggerBean has been set for
     *         specified dbName.
     * @throws RuntimeException if all matching AuthTriggerBeans threw
     *         RuntimeExceptions.  (This indicates that no matching
     *         AuthTriggerBean functioned properly, not that authentication was
     *         purposefully denied by any AuthTriggerBean).
     * @throws Exception (non-runtime).  A matching AuthTriggerBean threw this
     *         Exception.
     * @return Null or java.sql.Array to indicate successful authentication
     *         according to the contract for HyperSQL authentication functions.
     */
    public static java.sql.Array authenticate(
            String database, String user, String password)
            throws Exception {
        List<AuthTriggerBean> beanList =
                AuthBeanMultiplexer.beans.get(database);
        if (beanList == null) {
            throw new IllegalArgumentException("Database '" + database
                    + "' has not been set up with "
                    + AuthBeanMultiplexer.class.getName());
        }
        Exception firstRTE = null;
        String[] beanRet;
        for (AuthTriggerBean nextBean : beanList) try {
            beanRet = nextBean.authenticate(database, user, password);
            return (beanRet == null)
                    ? null : new JDBCArrayBasic(beanRet, Type.SQL_VARCHAR);
        } catch (RuntimeException re) {
            if (firstRTE == null) {
                firstRTE = re;
            }
            // TODO:  Write an application log entry and proceed
        } catch (Exception e) {
            throw e;
        }
        throw firstRTE;
    }
}
