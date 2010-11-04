package org.hsqldb.auth;

/**
 * N.b. AuthFunctionBeans are NOT directly usable as HyperSQL Authentication
 * Function methods, they are POJO beans to be managed by AuthBeanMultiplexer
 * (which does have a real HyperSQL Authentication Function static method).
 *
 * @see AuthBeanMultiplexer for how these beans are used.
 */
public interface AuthFunctionBean {
    /**
     * Return a list of authorized roles or null to indicate that the
     * implementation does not intend to produce a specific role list but only
     * to indicate whether to allow access or not.
     * A return value of String[0] is different from returning null, and means
     * that the user should not be granted any roles.
     *
     * @throws Exception If user should not be allowed access to the specified
     *         database.  Other registed AuthFunctionBeans will not be attempted.
     * @throws RuntimeException Upon system problem.  The exception will be
     *         logged to the HyperSQL application logger and other registered
     *         AuthFunctionBeans (if any) will be attempted.
     * @return null or String[] according to the contract of HyperSQL
     *         authentication function contract, except that the role/schema
     *         list is returned as a String[] instead of a java.sql.Array.
     */
    public String[] authenticate(
            String dbName, String userName, String password) throws Exception;
}
