/* Copyright (c) 2001-2022, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb.index;

import org.hsqldb.HsqlException;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.OrderedLongHashSet;
import org.hsqldb.map.BitMap;
import org.hsqldb.persist.DataFileCache;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.persist.RowStoreAVL;
import org.hsqldb.result.Result;
import org.hsqldb.rowio.RowInputBinary;

/**
 * Checks indexes for inconsistencies
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.1
 * @since 2.5.1
 */
public class IndexAVLCheck {

    public static Result checkAllTables(Session session, int type) {

        Result result = IndexStats.newEmptyResult();
        HsqlArrayList allTables =
            session.database.schemaManager.getAllTables(true);
        int tableCount = allTables.size();

        for (int i = 0; i < tableCount; i++) {
            Table table = (Table) allTables.get(i);

            if (!table.isCached()) {
                continue;
            }

            checkTable(session, table, result, type);
        }

        return result;
    }

    public static Result checkTable(Session session, Table table, int type) {

        Result result = IndexStats.newEmptyResult();

        if (!table.isCached()) {
            return result;
        }

        checkTable(session, table, result, type);

        return result;
    }

    public static void checkTable(Session session, Table table, Result result,
                                  int type) {

        RowStoreAVL tableStore =
            (RowStoreAVL) table.database.persistentStoreCollection.getStore(
                table);
        IndexStats[] statList = tableStore.checkIndexes(session, type);

        statList[0].addTableStats(result);

        for (int j = 0; j < statList.length; j++) {
            statList[j].addStats(result);
        }

        if (type == IndexStats.fixAll) {
            boolean hasErrors = false;

            for (int j = 0; j < statList.length; j++) {
                if (statList[j].hasErrors) {
                    hasErrors = true;
                }
            }

            if (hasErrors) {
                reindexTable(session, table, tableStore, statList);

                for (int j = 0; j < statList.length; j++) {
                    if (statList[j].reindexed) {
                        statList[j].addReindexedStats(result);
                    }
                }
            }
        }
    }

    public static void reindexTable(Session session, Table table,
                                    PersistentStore store,
                                    IndexStats[] indexStats) {

        Index   readIndex = null;
        boolean reindex   = false;

        for (int i = 0; i < indexStats.length; i++) {
            if (!indexStats[i].hasErrors) {
                readIndex = table.getIndex(i);

                break;
            }
        }

        if (readIndex == null) {
            session.database.logger.logSevereEvent(
                "could not recreate damaged indexes for table: "
                + table.getName().statementName, null);

            return;
        }

        for (int i = 0; i < indexStats.length; i++) {
            if (indexStats[i].hasErrors) {
                Index index = table.getIndex(i);

                store.reindex(session, index, readIndex);

                indexStats[i].reindexed = true;
                reindex                 = true;
            }
        }

        if (reindex) {
            session.database.logger.logSevereEvent(
                "recreated damaged indexes for table: "
                + table.getName().statementName, null);
        }
    }

    public static class IndexAVLProbe {

        static final int      maxDepth = 16;    // must be half of 32 (max depth)
        final int             fileBlockItemCount;
        final int             cacheScale;
        final Session         session;
        final PersistentStore store;
        final IndexAVL        index;
        final NodeAVLDisk     rootNode;

        //
        IntKeyHashMap      bitMaps;
        IntKeyHashMap      bitMapsPos;
        OrderedLongHashSet badRows;
        OrderedLongHashSet loopedRows;
        OrderedLongHashSet ignoreRows;
        HsqlArrayList      unorderedRows = new HsqlArrayList();
        int                branchPosition;
        int                leafPosition;
        long               errorRowCount;
        long               rowCount;
        long               loopCount;
        boolean            printErrors = false;

        /**
         * Uses one arraylist for the nodes near the root and another array list
         * for leaves.
         *
         * Returns nodes to depth of 32
         */
        public IndexAVLProbe(Session session, PersistentStore store,
                             IndexAVL index, NodeAVL rootNode) {

            DataFileCache cache = store.getCache();

            this.fileBlockItemCount = cache == null ? 0
                                                    : store.getCache()
                                                    .spaceManager
                                                        .getFileBlockItemCount();
            this.cacheScale = cache == null ? 0
                                            : store.getCache()
                                                .getDataFileScale();
            this.session  = session;
            this.store    = store;
            this.index    = index;
            this.rootNode = cache == null ? null
                                          : (NodeAVLDisk) rootNode;
        }

        public IndexStats getStats() {

            IndexStats stats = new IndexStats();

            stats.index         = index;
            stats.store         = store;
            stats.errorCount    = errorRowCount;
            stats.loopCount     = loopCount;
            stats.goodRowCount  = rowCount;
            stats.unorderedList = unorderedRows;
            stats.hasErrors     = hasErrors();

            return stats;
        }

        public boolean hasErrors() {
            return !(errorRowCount == 0 && loopCount == 0
                     && unorderedRows.isEmpty());
        }

        public void probe() {

            if (index == null) {
                return;
            }

            if (rootNode == null) {
                return;
            }

            if (fileBlockItemCount == 0) {
                return;
            }

            bitMaps       = new IntKeyHashMap();
            bitMapsPos    = new IntKeyHashMap();
            badRows       = new OrderedLongHashSet();
            loopedRows    = new OrderedLongHashSet();
            ignoreRows    = new OrderedLongHashSet();
            unorderedRows = new HsqlArrayList();

            Row row = rootNode.getRow(store);

            setSpaceForRow(row);

            // fill the root and set any branch roots
            getNodesFrom(0, rootNode, true);

            if (!hasErrors()) {
                checkIndexOrder();
            }
        }

        public void checkIndexOrder() {

            int errors = 0;

            store.readLock();

            try {
                NodeAVL p = index.getAccessor(store);
                NodeAVL f = null;

                while (p != null) {
                    f = p;
                    p = p.getLeft(store);
                }

                p = f;

                while (f != null) {
                    errors += checkNodes(f, unorderedRows);

                    NodeAVL fnext = index.next(store, f);

                    if (fnext != null) {
                        int c = index.compareRowForInsertOrDelete(session,
                            fnext.getRow(store), f.getRow(store), true, 0);

                        if (c <= 0) {
                            if (errors < 10) {
                                unorderedRows.add("broken index order ");
                            }

                            errors++;
                        }
                    }

                    f = fnext;
                }
            } finally {
                store.readUnlock();
            }
        }

        int checkNodes(NodeAVL p, HsqlArrayList list) {

            NodeAVLDisk l      = (NodeAVLDisk) p.getLeft(store);
            NodeAVLDisk r      = (NodeAVLDisk) p.getRight(store);
            int         errors = 0;

            if (l != null && l.iBalance == -2) {
                list.add("broken index - deleted");

                errors++;
            }

            if (r != null && r.iBalance == -2) {
                list.add("broken index -deleted");

                errors++;
            }

            if (l != null && p.getPos() != l.getParentPos()) {
                list.add("broken index - no parent");

                errors++;
            }

            if (r != null && p.getPos() != r.getParentPos()) {
                list.add("broken index - no parent");

                errors++;
            }

            return errors;
        }

        public TableBase getCurrentTable() {
            return index.getTable();
        }

        public long getErrorCount() {
            return errorRowCount;
        }

        public IntKeyHashMap getBitMaps() {
            return bitMaps;
        }

        public OrderedLongHashSet getBadRowPosList() {
            return badRows;
        }

        /*
         * todo -
         *
         * this goes from root to branch in one direction. we can also check the
         * parent node of each node and see if it leads to a differen node not
         * yet found - this will find errors with AVL and saving of rows
         */

        /**
         * goes to maxDepth
         * include is false to exclude a node that has readable index pointers but unreadable data
         */
        private void getNodesFrom(int depth, NodeAVLDisk node,
                                  boolean include) {

            if (node == null) {
                return;
            }

            long pos = node.getPos();

            if (!recordRowPos(pos)) {
                loopedRows.add(pos);

                return;
            }

            rowCount++;

            // the children of the node are checked but the row is ignored
            if (!include) {
                ignoreRows.add(pos);
            }

            long leftPos = node.getLeftPos();

            try {
                if (badRows.contains(leftPos)) {
                    return;
                }

                NodeAVLDisk next = (NodeAVLDisk) node.getLeft(store);

                if (next != null) {
                    Row row = next.getRow(store);

                    // avoid overwitten
                    if (setSpaceForRow(row)) {
                        getNodesFrom(depth + 1, next, true);

                        if (next.getParentPos() != pos) {
                            NodeAVLDisk parentNode =
                                (NodeAVLDisk) node.getParent(store);

                            loopCount++;
                        }
                    } else {
                        badRows.add(leftPos);
                    }
                }
            } catch (HsqlException e) {
                RowInputBinary rowIn = (RowInputBinary) e.info;

                if (rowIn != null) {
                    rowIn.ignoreDataErrors = true;

                    try {
                        NodeAVLDisk next = (NodeAVLDisk) node.getLeft(store);

                        getNodesFrom(depth + 1, next, false);
                    } catch (Throwable t) {
                        badRows.add((int) leftPos);
                    } finally {
                        rowIn.ignoreDataErrors = false;
                    }
                }

                errorRowCount++;
            } catch (Throwable t) {
                errorRowCount++;
            }

            long rightPos = node.getRightPos();

            try {
                if (badRows.contains(rightPos)) {
                    return;
                }

                NodeAVLDisk next = (NodeAVLDisk) node.getRight(store);

                if (next != null) {
                    Row row = next.getRow(store);

                    // avoid loops
                    if (setSpaceForRow(row)) {
                        getNodesFrom(depth + 1, next, true);

                        if (next.getParentPos() != pos) {
                            NodeAVLDisk parentNode =
                                (NodeAVLDisk) node.getParent(store);

                            loopCount++;
                        }
                    } else {
                        badRows.add(rightPos);
                    }
                }
            } catch (HsqlException e) {
                RowInputBinary rowIn = (RowInputBinary) e.info;

                if (rowIn != null) {
                    rowIn.ignoreDataErrors = true;

                    try {
                        NodeAVLDisk next = (NodeAVLDisk) node.getRight(store);

                        getNodesFrom(depth + 1, next, false);
                    } catch (Throwable t) {
                        badRows.add(rightPos);
                    } finally {
                        rowIn.ignoreDataErrors = false;
                    }
                }

                errorRowCount++;
            } catch (Throwable t) {
                errorRowCount++;
            }
        }

        /**
         * returns false if a node appears more than once in the index
         */
        boolean setSpaceForRow(Row object) {

            long    position = object.getPos();
            int     units    = object.getStorageSize() / cacheScale;
            boolean result   = true;

            for (; units > 0; ) {
                int blockIndex   = (int) (position / fileBlockItemCount);
                int blockOffset  = (int) (position % fileBlockItemCount);
                int currentUnits = fileBlockItemCount - blockOffset;

                if (currentUnits > units) {
                    currentUnits = units;
                }

                BitMap bitMap   = getBitMap(blockIndex);
                int    countSet = bitMap.countSet(blockOffset, currentUnits);

                if (countSet > 0) {

                    // two rows overlap (if only one index is adding to bit map)
                    if (printErrors) {
                        System.out.println(
                            "index scan - row duplicate in file block "
                            + blockIndex + " offset " + blockOffset);
                    }

                    result = false;
                } else {
                    bitMap.setRange(blockOffset, currentUnits);
                }

                units    -= currentUnits;
                position += currentUnits;
            }

            return result;
        }

        BitMap getBitMap(int blockIndex) {

            BitMap bitMap = (BitMap) bitMaps.get(blockIndex);

            if (bitMap == null) {
                bitMap =
                    new BitMap(new int[fileBlockItemCount / Integer.SIZE]);

                bitMaps.put(blockIndex, bitMap);
            }

            return bitMap;
        }

        boolean recordRowPos(long position) {

            int    blockIndex  = (int) (position / fileBlockItemCount);
            int    blockOffset = (int) (position % fileBlockItemCount);
            BitMap bitMap      = getPosSet(blockIndex);

            if (bitMap.isSet(blockOffset)) {
                return false;
            }

            bitMap.set(blockOffset);

            return true;
        }

        BitMap getPosSet(int blockIndex) {

            BitMap bitMap = (BitMap) bitMapsPos.get(blockIndex);

            if (bitMap == null) {
                bitMap =
                    new BitMap(new int[fileBlockItemCount / Integer.SIZE]);

                bitMapsPos.put(blockIndex, bitMap);
            }

            return bitMap;
        }
    }
}
