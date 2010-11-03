package org.hsqldb.auth;

/**
 * N.b. AuthTriggerBeans are NOT directly usable as HyperSQL Triggers, they are
 * POJO beans to be managed by AuthBeanMultiplexer (which is a HyperSQL
 * Trigger).
 */
public interface AuthTriggerBean {
    /**
     * Return a list of authorized roles or null to indicate that the
     * implementation does not intend to produce a specific role list but only
     * to indicate whether to allow access or not.
     * A return value of String[0] is different from returning null, and means
     * that the user should not be granted any roles.
     *
     * @throws Exception If user should not be allowed access to the specified
     *         database.
     * @throws RuntimeException Upon system problem.  The exception will be
     *         logged to the HyperSQL application logger but will otherwise be
     *         handled the same as a non-runtime Exception to deny access.
     */
    public String[] authenticate(
            String dbName, String userName, String password) throws Exception;
}
