
package org.hsqldb.testbase;

import java.util.EventListener;

/**
 * for event {@link ConnectionFactory#closeRegisteredObjects()}.
 */
public interface ConnectionFactoryEventListener extends EventListener {

    /**
     *
     * @param source the connection factory that has finished
     */
    void registeredObjectsClosed(ConnectionFactory source);
    
}
