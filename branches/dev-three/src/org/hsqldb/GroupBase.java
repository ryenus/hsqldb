package org.hsqldb;

import org.hsqldb.lib.IntKeyHashMap;

public class GroupBase {
    public IntKeyHashMap baseColumns;
    public boolean isBasic = true;

    public GroupBase(Expression[] expressions, Object[] exprColumns, int index) {
        baseColumns = new IntKeyHashMap();
        for (int i = 0; i < expressions.length; i++) {
            addToBaseColumns(expressions[i], exprColumns, index);
        }
    }

    private void addToBaseColumns(Expression e, Object[] exprColumns, int indexLimitVisible) {
        if (e.groupingType != 0) {
            isBasic = false;
        }
        if (e.opType == OpTypes.NONE) {
            return;
        }
        if (e.nodes.length == 0) {
            boolean added = false;
            for (int i = 0; i < indexLimitVisible; i++) {
                Expression expr = (Expression) exprColumns[i];
                String alias = e.getAlias();
                if (alias.equals(expr.getColumnName()) || alias.equals(expr.getAlias())) {
                    baseColumns.put(expr.getAlias().hashCode(), e);
                    added = true;
                    break;
                }
            }
            if (!added) {
                baseColumns.put(e.getAlias().hashCode(), e);
            }
        } else {
            if (e.opType == OpTypes.ROW || e.opType == OpTypes.VALUELIST) {
                for (int i = 0; i < e.nodes.length; i++) {
                    addToBaseColumns(e.nodes[i], exprColumns, indexLimitVisible);
                }
            } else {
                baseColumns.put(e.getSQL().hashCode(), e);
                return;
            }
        }
    }
}