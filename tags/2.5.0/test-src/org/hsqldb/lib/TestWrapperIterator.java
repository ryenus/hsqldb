/*
 * Copyright (c) 2014, The HSQL Development Group All rights reserved. Redistribution and use in source and
 * binary forms, with or without modification, are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list of conditions and the
 * following disclaimer. Redistributions in binary form must reproduce the above copyright notice, this list
 * of conditions and the following disclaimer in the documentation and/or other materials provided with the
 * distribution. Neither the name of the HSQL Development Group nor the names of its contributors may be used
 * to endorse or promote products derived from this software without specific prior written permission. THIS
 * SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG, OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.hsqldb.lib;

import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.WrapperIterator;

public class TestWrapperIterator extends junit.framework.TestCase
{
    public void test()
    {
        WrapperIterator iter1 = new WrapperIterator();
        assertFalse( iter1.hasNext() );

        WrapperIterator iter2 = new WrapperIterator( "tutego" );
        assertTrue( iter2.hasNext() );
        assertEquals( "tutego", iter2.next() );
        assertFalse( iter2.hasNext() );

        String[] array = { "a", "b" };
        WrapperIterator iter3 = new WrapperIterator( array );
        assertTrue( iter3.hasNext() );
        assertEquals( "a", iter3.next() );
        assertTrue( iter3.hasNext() );
        assertEquals( "b", iter3.next() );
        assertFalse( iter3.hasNext() );
        
        Iterator iter4 = new HsqlArrayList( new String[] {"a", "b"}, 2 ).iterator();
        Iterator iter5 = new HsqlArrayList( new String[] {"c", "d"}, 2 ).iterator();
        WrapperIterator iter6 = new WrapperIterator(iter4, iter5);
        assertEquals( "a", iter6.next() );
        assertEquals( "b", iter6.next() );
        assertEquals( "c", iter6.next() );
        assertEquals( "d", iter6.next() );
    }
}
