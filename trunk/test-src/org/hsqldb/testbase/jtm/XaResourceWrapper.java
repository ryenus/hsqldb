
package org.hsqldb.testbase.jtm;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 *
 * @author campbell
 */
public class XaResourceWrapper implements XAResource{
    XAResource delegate;

    @Override
    public void commit(Xid xid, boolean bln) throws XAException {
        delegate.commit(xid, bln);
    }

    @Override
    public void end(Xid xid, int i) throws XAException {
        delegate.end(xid, i);
    }

    @Override
    public void forget(Xid xid) throws XAException {
        delegate.forget(xid);
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return delegate.getTransactionTimeout();
    }

    @Override
    public boolean isSameRM(XAResource xar) throws XAException {
        return delegate.isSameRM(xar);
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        return delegate.prepare(xid);
    }

    @Override
    public Xid[] recover(int i) throws XAException {
        return delegate.recover(i);
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        delegate.rollback(xid);
    }

    @Override
    public boolean setTransactionTimeout(int i) throws XAException {
        return delegate.setTransactionTimeout(i);
    }

    @Override
    public void start(Xid xid, int i) throws XAException {
        delegate.start(xid, i);
    }
    
    
}
