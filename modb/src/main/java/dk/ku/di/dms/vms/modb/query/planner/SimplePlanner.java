package dk.ku.di.dms.vms.modb.query.planner;

import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.enums.GroupByOperationEnum;
import dk.ku.di.dms.vms.modb.api.query.enums.OrderBySortOrderEnum;
import dk.ku.di.dms.vms.modb.definition.ColumnReference;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.GroupByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.OrderByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.execution.operators.AbstractSimpleOperator;
import dk.ku.di.dms.vms.modb.query.execution.operators.IndexMultiAggregateScan;
import dk.ku.di.dms.vms.modb.query.execution.operators.count.IndexCount;
import dk.ku.di.dms.vms.modb.query.execution.operators.count.IndexCountGroupBy;
import dk.ku.di.dms.vms.modb.query.execution.operators.minmax.IndexGroupByMax;
import dk.ku.di.dms.vms.modb.query.execution.operators.minmax.IndexGroupByMin;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.*;
import dk.ku.di.dms.vms.modb.query.execution.operators.sum.IndexSum;
import dk.ku.di.dms.vms.modb.query.execution.operators.sum.Sum;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Planner that only takes into consideration simple read and write queries.
 * Those involving single tables and filters, and no
 * multiple aggregations or joins with more than 2 tables.
 */
public final class SimplePlanner {

    public SimplePlanner(){}

    /**
     * @param columnsToFilter additional columns to be filtered, but not on index
     */
    private record IndexSelectionVerdict(boolean indexIsUsedGivenWhereClause,
                                         IMultiVersionIndex index,
                                         int[] columnsToFilter) implements Comparable<IndexSelectionVerdict> {

        @Override
        public int compareTo(IndexSelectionVerdict o) {
            // do both indexes can be effectively applied?
//            if (this.indexIsUsedGivenWhereClause && o.indexIsUsedGivenWhereClause) {
//
//                // are both unique?
//                if (this.index.getType() == IndexTypeEnum.UNIQUE && o.index.getType() == IndexTypeEnum.UNIQUE) {
//                    // which one has more columns to filter? leading to more rows cut
//                    // that may not be correct given the selectivity of some values, simple heuristic here
//                    if (this.columnsToFilter.length > o.columnsToFilter.length) return 0;
//                    if (this.columnsToFilter.length < o.columnsToFilter.length) return 1;
//
//                    // which one has larger table?
//                    if (this.index.size() < o.index.size()) return 0;
//
//                } else if (this.index.getType() == IndexTypeEnum.UNIQUE) return 0;
//
//            } else if (this.indexIsUsedGivenWhereClause) {
//                return 0; // must be deep left then
//            }
            return 1;
        }

    }

    public AbstractSimpleOperator plan(QueryTree queryTree) {
        if(queryTree.isSimpleScan()){
            return this.planSimpleScan(queryTree);
        }
        if(queryTree.isSimpleScanWithOrder()){
            return this.planSimpleScanWithOrder(queryTree);
        }
        if(queryTree.isSimpleAggregate()){
            return this.planSimpleAggregate(queryTree);
        }
        if(queryTree.isSimpleJoin()){
            return this.planSimpleJoin(queryTree);
        }
        if(queryTree.hasMultipleJoins()){
            return this.planMultipleJoins(queryTree);
        }
        if(queryTree.hasMultipleAggregates()){
            return this.planMultipleAggregates(queryTree);
        }
        throw new RuntimeException("Could not find a plan for :"+queryTree);
    }

    private AbstractSimpleOperator planMultipleAggregates(QueryTree queryTree) {
        IMultiVersionIndex indexSelected = this.getOptimalIndex(
                queryTree.projections.getFirst().table,
                queryTree.wherePredicates).index();
        return new IndexMultiAggregateScan(queryTree.groupByProjections, indexSelected, queryTree.projections.stream().map(i->i.columnPosition).toList(), 0);
    }

    private AbstractSimpleOperator planSimpleJoin(QueryTree queryTree) {

        // define left deep
        JoinPredicate joinPredicate = queryTree.joinPredicates.getFirst();

        // the index with most columns applying to the probe is the deepest
        IndexSelectionVerdict indexForTable1 = this.getOptimalIndex(joinPredicate.getLeftTable(), queryTree.wherePredicates);

        IndexSelectionVerdict indexForTable2 = this.getOptimalIndex(joinPredicate.getRightTable(), queryTree.wherePredicates);

        // TODO finish

//        if(indexForTable1.compareTo( indexForTable2 ) == 0){
//            // should be left
//            if(indexForTable1.index.getType() == IndexTypeEnum.UNIQUE && indexForTable2.index.getType() == IndexTypeEnum.UNIQUE)
//                return new UniqueHashJoinWithProjection( indexForTable1.index, indexForTable2.index,
//                    null, null,
//                    null, null,
//                    null, 0 );
//            if(indexForTable1.index.getType() == IndexTypeEnum.UNIQUE)
//                return new UniqueHashJoinNonUniqueHashWithProjection( indexForTable1.index, indexForTable2.index,
//                        null, null,
//                        null, null,
//                        null, 0 );
//            throw new IllegalStateException("No support for join on non unique hash indexes!");
//        } else {
//            if(indexForTable2.index.getType() == IndexTypeEnum.UNIQUE && indexForTable1.index.getType() == IndexTypeEnum.UNIQUE)
//                return new UniqueHashJoinWithProjection( indexForTable2.index, indexForTable1.index,
//                    null, null,
//                    null, null,
//                    null, 0 );
//            if(indexForTable2.index.getType() == IndexTypeEnum.UNIQUE)
//                return new UniqueHashJoinNonUniqueHashWithProjection( indexForTable2.index, indexForTable1.index,
//                        null, null,
//                        null, null,
//                        null, 0 );
//            throw new IllegalStateException("No support for join on non unique hash indexes!");
//        }
        return null;
    }

    private AbstractSimpleOperator planMultipleJoins(QueryTree queryTree) {
        // order joins in order of join operation
        // simple heuristic, table with most records goes first, but care must be taken on precedence of foreign keys
        // dynamic programming. which join must execute first?
        // ordered by the number of records? index type?
        return null;
    }

    private AbstractSimpleOperator planSimpleAggregate(QueryTree queryTree) {
        // then just one since it is simple
        GroupByOperationEnum op = queryTree.groupByProjections.getFirst().groupByOperation();
        switch (op){
            case MIN, MAX -> {
                Table tb = queryTree.groupByProjections.getFirst().columnReference().table;
                IMultiVersionIndex indexSelected = this.getOptimalIndex(
                        queryTree.groupByProjections.getFirst().columnReference().table,
                        queryTree.wherePredicates).index();
                int[] indexColumns = queryTree.groupByColumns.stream()
                        .mapToInt(ColumnReference::getColumnPosition ).toArray();

                // assumed to be only one
                int minMaxColumn = queryTree.groupByProjections.stream().mapToInt( GroupByPredicate::columnPosition ).toArray()[0];

                final int[] projectionColumns = new int[queryTree.projections.size()+1];
                int idxCol = 0;
                for(ColumnReference column : queryTree.projections){
                    projectionColumns[idxCol] = column.getColumnPosition();
                    idxCol++;
                }
                projectionColumns[idxCol] = minMaxColumn;

                // calculate entry size... sum of the size of the column types of the projection
                int entrySize = calculateQueryResultEntrySize(tb.schema(), queryTree.projections.size()+1, projectionColumns);

                if(op == GroupByOperationEnum.MIN) {
                    return new IndexGroupByMin(entrySize, indexSelected, projectionColumns, tb.schema(),
                            indexColumns, minMaxColumn, queryTree.limit.orElse(Integer.MAX_VALUE));
                } else {
                    return new IndexGroupByMax(entrySize, indexSelected, projectionColumns, tb.schema(),
                            indexColumns, minMaxColumn, queryTree.limit.orElse(Integer.MAX_VALUE));
                }
            }
            case SUM -> {
                // check if there is an index that can be applied
                IMultiVersionIndex indexSelected = this.getOptimalIndex(
                        queryTree.groupByProjections.getFirst().columnReference().table,
                        queryTree.wherePredicates).index();
                if(indexSelected == null){
                    return new Sum(
                            queryTree.groupByProjections.getFirst().columnReference().dataType,
                            queryTree.groupByProjections.getFirst().columnReference().columnPosition,
                            queryTree.groupByProjections.getFirst().columnReference().table.underlyingPrimaryKeyIndex());
                }
                return new IndexSum(
                        queryTree.groupByProjections.getFirst().columnReference().dataType,
                        queryTree.groupByProjections.getFirst().columnReference().columnPosition,
                        // indexSelected
                        queryTree.groupByProjections.getFirst().columnReference().table.underlyingPrimaryKeyIndex()
                );
            }
            case COUNT -> {
                Table tb = queryTree.groupByProjections.getFirst().columnReference().table;
                IMultiVersionIndex indexSelected = this.getOptimalIndex(
                        queryTree.groupByProjections.getFirst().columnReference().table,
                        queryTree.wherePredicates
                        ).index();
                if(queryTree.groupByColumns.isEmpty()){
                    // then no group by
                    // how the user can specify a distinct?
                    return new IndexCount(
//                            indexSelected == null ? tb.underlyingPrimaryKeyIndex() :  indexSelected
                            tb.underlyingPrimaryKeyIndex()
                    );
                } else {
                    int[] columns = queryTree.groupByColumns.stream()
                            .mapToInt(ColumnReference::getColumnPosition).toArray();
                    return new IndexCountGroupBy(
                            // indexSelected == null ? tb.underlyingPrimaryKeyIndex() : indexSelected,
                            tb.underlyingPrimaryKeyIndex(),
                            columns
                    );
                }
            }
            default -> throw new IllegalStateException("Operator not yet implemented.");
        }
    }

    private AbstractScan planSimpleScanWithOrder(QueryTree queryTree) {
        Table tb = queryTree.projections.get(0).table;
        IndexSelectionVerdict indexSelectionVerdict = this.getOptimalIndex(tb, queryTree.wherePredicates);
        int[] projectionColumns = new int[queryTree.projections.size()];
        int idxCol = 0;
        for(ColumnReference column : queryTree.projections) {
            projectionColumns[idxCol] = column.getColumnPosition();
            idxCol++;
        }
        int entrySize = calculateQueryResultEntrySize(tb.schema(), queryTree.projections.size(), projectionColumns);
        OrderByPredicate orderByPredicate = queryTree.orderByPredicates.getFirst();
        if(indexSelectionVerdict.indexIsUsedGivenWhereClause()) {
            return new IndexScanWithOrder(indexSelectionVerdict.index(), projectionColumns,
                    orderByPredicate.columnReference.columnPosition, orderByPredicate.sortOperation == OrderBySortOrderEnum.DESC, entrySize);
        } else {
            if(orderByPredicate.sortOperation == OrderBySortOrderEnum.ASC && tb.primaryKeyIndex().isSorted()) {
                return new FullScan(tb.primaryKeyIndex(), projectionColumns, entrySize);
            }
            return new FullScanWithOrder(tb.primaryKeyIndex(), projectionColumns,
                    orderByPredicate.columnReference.columnPosition, orderByPredicate.sortOperation == OrderBySortOrderEnum.DESC, entrySize);
        }
    }

    private AbstractScan planSimpleScan(QueryTree queryTree) {
        // given it is simple, pick the table from one of the columns
        // must always have at least one projected column
        Table tb = queryTree.projections.get(0).table;
        // avoid one of the columns to have expression different from EQUALS
        // to be picked by unique and non-unique index
        IndexSelectionVerdict indexSelectionVerdict = this.getOptimalIndex(tb, queryTree.wherePredicates);
        // build projection
        int[] projectionColumns = new int[queryTree.projections.size()];
        int idxCol = 0;
        for(ColumnReference column : queryTree.projections){
            projectionColumns[idxCol] = column.getColumnPosition();
            idxCol++;
        }
        int entrySize = calculateQueryResultEntrySize(tb.schema(), queryTree.projections.size(), projectionColumns);
        if(indexSelectionVerdict.indexIsUsedGivenWhereClause()) {
            // return the index scan with projection
            return new IndexScan(indexSelectionVerdict.index(), projectionColumns, entrySize);
        } else {
            // then must get the PK index, ScanWithProjection
            return new FullScan(tb.primaryKeyIndex(), projectionColumns, entrySize);
        }
    }

    private static int calculateQueryResultEntrySize(Schema schema, int nProj, int[] projectionColumns) {
        int entrySize = 0;
        for(int i = 0; i < nProj; i++){
            entrySize += schema.columnDataType( projectionColumns[i] ).value;
        }
        return entrySize;
    }

    private IndexSelectionVerdict getOptimalIndex(final Table table, List<WherePredicate> wherePredicates) {
        final IntStream intStream = wherePredicates.stream()
                .filter( wherePredicate ->
                        ExpressionTypeEnum.equality(wherePredicate.expression)
                                && wherePredicate.columnReference.table.equals(table)
                )
                .mapToInt( WherePredicate::getColumnPosition );

        final int[] columnsForIndexSelection = intStream.toArray();

        if(columnsForIndexSelection.length == 0) {
            return new IndexSelectionVerdict(false, table.primaryKeyIndex(), columnsForIndexSelection);
        }

        final IIndexKey indexKey = KeyUtils.buildIndexKey(columnsForIndexSelection);

        // fast path (1): all columns are part of the primary index
        if (table.underlyingPrimaryKeyIndex().key().equals(indexKey)) {
            return new IndexSelectionVerdict(true, table.primaryKeyIndex(), columnsForIndexSelection);
        }

        // fast path (2): all columns are part of a secondary index
        if (table.secondaryIndexMap.containsKey(indexKey)) {
            return new IndexSelectionVerdict(
                    true,
                    table.secondaryIndexMap.get(indexKey),
                    columnsForIndexSelection);
        }

        if (table.partialIndexMap.containsKey(indexKey)) {
            return new IndexSelectionVerdict(
                    true,
                    table.partialIndexMap.get(indexKey),
                    columnsForIndexSelection);
        }
        final ReadWriteIndex<IKey> indexSelected = this.getOptimalIndex(table, columnsForIndexSelection);

        // is the index completely covered by the columns in the filter?
        if (indexSelected == null){
            // then just select the Primary index
            return new IndexSelectionVerdict(false, table.primaryKeyIndex(), columnsForIndexSelection);
        }

        // columns not in the index, but require filtering
        final IntStream filteredStream = Arrays.stream(columnsForIndexSelection).filter( w -> !indexSelected.containsColumn( w ) );
        final int[] filterColumns = filteredStream.toArray();

        // any column of the index is in the filter? if so, index is not used.
        // boolean indexColumnInFilter = Arrays.stream(columnsForIndexSelection).anyMatch(indexSelected::containsColumn);

        return new IndexSelectionVerdict(true,
                table.secondaryIndexMap.containsKey(indexSelected.key()) ?
                        table.secondaryIndexMap.get(indexSelected.key()) :
                        table.partialIndexMap.get(indexSelected.key())
                , filterColumns);
    }

    private ReadWriteIndex<IKey> getOptimalIndex(Table table, int[] filterColumns){
        // no index apply so far, perhaps a subset then?
        List<int[]> combinations = Combinatorics.getAllPossibleColumnCombinations(filterColumns);
        // heuristic: return the one that embraces more columns
        ReadWriteIndex<IKey> bestSoFar = null;
        int maxLength = 0;
        for(int[] arr : combinations) {
            IKey indexKey = KeyUtils.buildIndexKey(arr);
            if(table.secondaryIndexMap.get(indexKey) != null){
                if(arr.length > maxLength){
                    bestSoFar = table.secondaryIndexMap.get(indexKey).underlyingIndex();
                    maxLength = arr.length;
                }
            }
        }
        return bestSoFar;
    }

}