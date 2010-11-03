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
 * trigger method.
 * Manages a set of AuthTriggerBean implementations
 * <P>
 * Note that ANY bean in the collection approving access will result in access
 * being permitted, whereas if any bean denies access, the following beans in
 * the collection will be tried.
 * </P>
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
     * @see #setAUthTriggerBeans(List)
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
     * @see setAuthTriggerBean(String, AUthTriggerBean)
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
     * HyperSQL Trigger Method.
     * <OL>
     *   <LI>The return value from the first AuthTriggerBean registered for the
     *       specified dbName which does not throw will be returned.
     *   <LI>If there are no AuthTriggerBeans registered for the specified
     *       dbName, then this method will throw an IllegalArgumentException.
     *   <LI>If all AuthTriggerBeans registered for the specified dbName throw,
     *       then this method will rethrow (pass through) the first
     *       RuntimeException thrown (if any), or the first Exception thrown.
     * </OL>
     *
     * @See HyperSQL User Guide, section X.  TODO:  Add section name.
     * @throws IllegalArgumentException if no AuthTriggerBean has been set for
     *         specified dbName.
     */
    // TODO:  Update return type according to Fred's upcoming work
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
        Exception firstCaughtException = null;
        String[] beanRet;
        for (AuthTriggerBean nextBean : beanList) try {
            beanRet = nextBean.authenticate(database, user, password);
            return (beanRet == null)
                    ? null : new JDBCArrayBasic(beanRet, Type.SQL_VARCHAR);
        } catch (RuntimeException re) {
            if (firstCaughtException == null
                    || !(firstCaughtException instanceof RuntimeException)) {
                firstCaughtException = re;
            }
            // TODO:  Write an application log entry and proceed
        } catch (Exception e) {
            if (firstCaughtException == null) {
                firstCaughtException = e;
            }
        }
        throw firstCaughtException;
    }
}
