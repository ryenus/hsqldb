package org.hsqldb.testbase.jtm;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.UserTransaction;
import javax.transaction.xa.XAResource;

/**
 * Allows operations to be performed against the transaction in the target
 * Transaction object.
 *
 * A Transaction object is created corresponding to each global transaction
 * creation.
 *
 * The Transaction object can be used for resource enlistment, synchronization
 * registration, transaction completion, and status query operations.
 */
public class MockTransaction implements Transaction, UserTransaction {

    private static final Logger LOG = Logger.getLogger(MockTransaction.class.getName());
    private final Set<XAResource> enlistedResources;
    private int status = Status.STATUS_UNKNOWN;

    @SuppressWarnings("CollectionWithoutInitialCapacity")
    public MockTransaction() {
        this.enlistedResources = Collections.newSetFromMap(new IdentityHashMap<>());
    }

    /**
     * Complete the transaction represented by this Transaction object.
     *
     * @exception RollbackException          Thrown to indicate that
     *                                       the transaction has been rolled back rather than committed.
     *
     * @exception HeuristicMixedException    Thrown to indicate that a heuristic
     *                                       decision was made and that some relevant updates have been committed
     *                                       while others have been rolled back.
     *
     * @exception HeuristicRollbackException Thrown to indicate that a
     *                                       heuristic decision was made and that all relevant updates have been
     *                                       rolled back.
     *
     * @exception SecurityException          Thrown to indicate that the thread
     *                                       is
     *                                       not allowed to commit the transaction.
     *
     * @exception IllegalStateException      Thrown if the transaction in the
     *                                       target object is inactive.
     *
     * @exception SystemException            Thrown if the transaction manager
     *                                       encounters an unexpected error condition.
     */
    @Override
    public void commit() throws RollbackException,
            HeuristicMixedException, HeuristicRollbackException,
            SecurityException, IllegalStateException, SystemException {

        Iterator<XAResource> itr = enlistedResources.iterator();
        
        while(itr.hasNext()) {
            XAResource xaRes = itr.next();
            
            //xaRes.

        }

    }

    /**
     * Disassociate the resource specified from the transaction associated
     * with the target Transaction object.
     *
     * @param xaRes The XAResource object associated with the resource
     *              (connection).
     *
     * @param flag  One of the values of TMSUCCESS, TMSUSPEND, or TMFAIL.
     *
     * @exception IllegalStateException Thrown if the transaction in the
     *                                  target object is inactive.
     *
     * @exception SystemException       Thrown if the transaction manager
     *                                  encounters an unexpected error condition.
     *
     * @return <i>true</i> if the resource was delisted successfully; otherwise
     * <i>false</i>.
     *
     */
    @Override
    public boolean delistResource(XAResource xaRes, int flag)
            throws IllegalStateException, SystemException {
        return true;
    }

    /**
     * Enlist the resource specified with the transaction associated with the
     * target Transaction object.
     *
     * @param xaRes The XAResource object associated with the resource
     *              (connection).
     *
     * @return <i>true</i> if the resource was enlisted successfully; otherwise
     * <i>false</i>.
     *
     * @exception RollbackException     Thrown to indicate that
     *                                  the transaction has been marked for rollback only.
     *
     * @exception IllegalStateException Thrown if the transaction in the
     *                                  target object is in the prepared state or the transaction is
     *                                  inactive.
     *
     * @exception SystemException       Thrown if the transaction manager
     *                                  encounters an unexpected error condition.
     *
     */
    @Override
    public boolean enlistResource(XAResource xaRes)
            throws RollbackException, IllegalStateException,
            SystemException {

        if (this.enlistedResources.contains(xaRes)) {

        }
        return true;
    }

    /**
     * Obtain the status of the transaction associated with the target
     * Transaction object.
     *
     * @return The transaction status. If no transaction is associated with
     *         the target object, this method returns the
     *         Status.NoTransaction value.
     *
     * @exception SystemException Thrown if the transaction manager
     *                            encounters an unexpected error condition.
     *
     */
    @Override
    public int getStatus() throws SystemException {
        return status;
    }

    /**
     * Register a synchronization object for the transaction currently
     * associated with the target object. The transction manager invokes
     * the beforeCompletion method prior to starting the two-phase transaction
     * commit process. After the transaction is completed, the transaction
     * manager invokes the afterCompletion method.
     *
     * @param sync The Synchronization object for the transaction associated
     *             with the target object.
     *
     * @exception RollbackException     Thrown to indicate that
     *                                  the transaction has been marked for rollback only.
     *
     * @exception IllegalStateException Thrown if the transaction in the
     *                                  target object is in the prepared state or the transaction is
     *                                  inactive.
     *
     * @exception SystemException       Thrown if the transaction manager
     *                                  encounters an unexpected error condition.
     *
     */
    @Override
    public void registerSynchronization(Synchronization sync)
            throws RollbackException, IllegalStateException,
            SystemException {

    }

    /**
     * Rollback the transaction represented by this Transaction object.
     *
     * @exception IllegalStateException Thrown if the transaction in the
     *                                  target object is in the prepared state or the transaction is
     *                                  inactive.
     *
     * @exception SystemException       Thrown if the transaction manager
     *                                  encounters an unexpected error condition.
     *
     */
    @Override
    public void rollback() throws IllegalStateException, SystemException {

    }

    /**
     * Modify the transaction associated with the target object such that
     * the only possible outcome of the transaction is to roll back the
     * transaction.
     *
     * @exception IllegalStateException Thrown if the target object is
     *                                  not associated with any transaction.
     *
     * @exception SystemException       Thrown if the transaction manager
     *                                  encounters an unexpected error condition.
     *
     */
    @Override
    public void setRollbackOnly() throws IllegalStateException,
            SystemException {
        this.status = Status.STATUS_MARKED_ROLLBACK;
    }

    @Override
    public void begin() throws NotSupportedException, SystemException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void setTransactionTimeout(int seconds) throws SystemException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
}
