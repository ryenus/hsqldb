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
 * Manages a set of AuthFunctionBean implementations
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
     * @see #setAuthFunctionBeans(List)
     */
    private static Map<String, List<AuthFunctionBean>> beans =
            new HashMap<String, List<AuthFunctionBean>>();

    public static AuthBeanMultiplexer getSingleton() {
        return singleton;
    }

    /**
     * Clear the set of AuthFunctionBeans
     */
    public void clear() {
        AuthBeanMultiplexer.beans.clear();
    }

    /**
     * Primary purpose of this class is to manage this static map.
     * From dbNames to ordered-lists-of-AuthFunctionBeans.
     * This is not an "adder" function, but a "setter" function, so do not use
     * this to add to a partial set, but to assign the entire set.
     * <P>
     * The given entries are copied, to limit side-effects and concurrency
     * issues.
     * </P>
     */
    public void setAuthFunctionBeans(
            Map<String, List<AuthFunctionBean>> authFunctionBeanMap) {
        if (AuthBeanMultiplexer.beans.size() > 0)
            throw new IllegalStateException(
                    "Use setAuthFunctionBeans(Map) only when the set is empty");
        AuthBeanMultiplexer.beans.putAll(authFunctionBeanMap);
    }

    /**
     * This is not an "adder" function, but a "setter" function for the
     * specified dbName , so do not use this to add to a database's
     * FunctionBeans, but to assign the entire list for that database.
     * <P>
     * The given entries are copied, to limit side-effects and concurrency
     * issues.
     * </P> <P>
     * Use this method instead of setAuthFunctionBean(String, AuthFunctionBean)
     * in order to set up multiple authenticators for a single database for
     * redundancy purposes.
     * </P>
     *
     * @see setAuthFunctionBeans(Map)
     * @see setAuthFunctionBean(String, AuthFunctionBean)
     */
    public void setAuthFunctionBeans(String dbName,
            List<AuthFunctionBean> authFunctionBeans) {
        List<AuthFunctionBean> dbsBeans = AuthBeanMultiplexer.beans.get(dbName);
        if (dbsBeans == null) {
            dbsBeans = new ArrayList<AuthFunctionBean>();
            AuthBeanMultiplexer.beans.put(dbName, dbsBeans);
        } else {
            if (dbsBeans.size() > 0)
                throw new IllegalStateException(
                        "Use setAuthFunctionBeans(String, List) only when the "
                        + "db's AuthFunctionBean list is empty");
        }
        dbsBeans.addAll(authFunctionBeans);
    }

    /**
     * This is not an "adder" function, but a "setter" function for the
     * specified dbName , so do not use this to add to a database's
     * FunctionBeans, but to assign ths single given AuthFunctionBean as the
     * specified database's authenticator.
     * <P>
     * To set up multiple authenticators for a single database for redundancy
     * purposes, use the method setAuthFunctionBeans(String, List) instead.
     * </P>
     *
     * @see setAuthFunctionBeans(String, List)
     */
    public void setAuthFunctionBean(String dbName,
            AuthFunctionBean authFunctionBean) {
        setAuthFunctionBeans(
                dbName, Collections.singletonList(authFunctionBean));
    }

    /**
     * HyperSQL Java Function Method.
     * <P>
     * Registered AuthFunctionBeans matching the specified database and password
     * will be tried in order.
     * <OL>
     *   <li>If the AuthFunctionBean being tried throws a non-runtime Exception,
     *       then that RuntimeException is passed through (re-thrown), resulting
     *       in a SQLException for the authenticating application.
     *   <li>If the AuthFunctionBean being tried doesn't throw anything, then
     *       the return value is passed through (returned) and HyperSQL will
     *       allow access and set roles according to HyperSQL's authentication
     *       function contract.
     *   <LI>If the AuthFunctionBean being tried throws a RuntimeException, then
     *       the next AuthFunctionBean in turn will be tried.
     *       If all matching AuthFunctionBeans throw RuntimeExceptions, then the
     *       first RuntimeException that was thrown will be passed through
     *       (re-thrown), resulting in a SQLException for the authenticating
     *       application.
     *   <LI>If there are no AuthFunctionBeans registered for the specified
     *       dbName, then this method will throw an IllegalArgumentException,
     *       resulting in a SQLException for the authenticating application.
     * </OL>
     *
     * @See HyperSQL User Guide, System Management and Deployment Issues
     *         chapter, Authentication Settings subsection.
     * @throws IllegalArgumentException if no AuthFunctionBean has been set for
     *         specified dbName.
     * @throws RuntimeException if all matching AuthFunctionBeans threw
     *         RuntimeExceptions.  (This indicates that no matching
     *         AuthFunctionBean functioned properly, not that authentication was
     *         purposefully denied by any AuthFunctionBean).
     * @throws Exception (non-runtime).  A matching AuthFunctionBean threw this
     *         Exception.
     * @return Null or java.sql.Array to indicate successful authentication
     *         according to the contract for HyperSQL authentication functions.
     */
    public static java.sql.Array authenticate(
            String database, String user, String password)
            throws Exception {
        List<AuthFunctionBean> beanList =
                AuthBeanMultiplexer.beans.get(database);
        if (beanList == null) {
            throw new IllegalArgumentException("Database '" + database
                    + "' has not been set up with "
                    + AuthBeanMultiplexer.class.getName());
        }
        Exception firstRTE = null;
        String[] beanRet;
        for (AuthFunctionBean nextBean : beanList) try {
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
