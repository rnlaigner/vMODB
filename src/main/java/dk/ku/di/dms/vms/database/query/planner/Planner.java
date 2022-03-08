package dk.ku.di.dms.vms.database.query.planner;

import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.database.query.planner.node.filter.*;
import dk.ku.di.dms.vms.database.query.planner.node.join.*;
import dk.ku.di.dms.vms.database.query.planner.node.projection.Projector;
import dk.ku.di.dms.vms.database.query.planner.node.scan.IndexScan;
import dk.ku.di.dms.vms.database.query.planner.node.scan.SequentialScan;
import dk.ku.di.dms.vms.database.query.planner.utils.IdentifiableNode;
import dk.ku.di.dms.vms.database.store.common.CompositeKey;
import dk.ku.di.dms.vms.database.store.common.SimpleKey;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;
import dk.ku.di.dms.vms.database.store.index.IndexDataStructureEnum;
import dk.ku.di.dms.vms.database.store.common.IKey;
import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.*;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.database.query.planner.node.join.JoinTypeEnum.*;

/**
 * Class responsible for deciding for a plan to run a query
 * https://www.interdb.jp/pg/pgsql03.html
 * "The planner receives a query tree from the rewriter and generates a (query)
 *   plan tree that can be processed by the executor most effectively."
 */
public final class Planner {

    // TODO we can also have the option: AUTO, meaning the planner will look for the possibility of building a bushy tree
    public PlanNode plan(final QueryTree queryTree) throws FilterBuilderException {
        return this.plan( queryTree, QueryTreeTypeEnum.LEFT_DEEP );
    }

    // plan and optimize in the same step
    public PlanNode plan(final QueryTree queryTree, final QueryTreeTypeEnum treeType) throws FilterBuilderException {

        final Map<String,Integer> tablesInvolvedInJoin = new HashMap<>();

        for(final JoinPredicate join : queryTree.joinPredicates){
            tablesInvolvedInJoin.put(join.getLeftTable().getName(), 1);
            tablesInvolvedInJoin.put(join.getRightTable().getName(), 1);
        }

        // TODO optimization. avoid stream by reusing the table list in query tree. simply maintain a bool (in_join?)
        //  other approach is delivering these maps to the planner from the analyzer...

        final Map<Table,List<WherePredicate>> whereClauseGroupedByTableNotInAnyJoin =
                queryTree
                        .wherePredicates.stream()
                        .filter( clause -> !tablesInvolvedInJoin
                                .containsKey(clause.getTable().getName()) )
                        .collect(
                                Collectors.groupingBy( WherePredicate::getTable,
                                         Collectors.toList())
                        );

        final Map<Table,List<WherePredicate>> filtersForJoinGroupedByTable =
                queryTree
                        .wherePredicates.stream()
                        .filter( clause -> tablesInvolvedInJoin
                                .containsKey(clause.getTable().getName()) )
                        .collect(
                                Collectors.groupingByConcurrent( WherePredicate::getTable,
                                        Collectors.toList())
                        );

        final Map<Table,List<AbstractJoin>> joinsPerTable = new HashMap<>();
        final Map<Table,List<AbstractJoin>> auxJoinsPerTable = new HashMap<>();

        // TODO I am not considering a JOIN predicate may contain more than a column....
        // if the pk or secIndex only applies to one of the columns, then others become filters
        for(final JoinPredicate joinPredicate : queryTree.joinPredicates){

            Table tableLeft = joinPredicate.getLeftTable();

            // same approach used for composite key
            int[] colPosArr = { joinPredicate.columnLeftReference.columnPosition };

            final Optional<AbstractIndex<IKey>> leftIndexOptional = findOptimalIndex( tableLeft, colPosArr );

            Table tableRight = joinPredicate.getRightTable();

            colPosArr = new int[]{ joinPredicate.columnRightReference.columnPosition };

            Optional<AbstractIndex<IKey>> rightIndexOptional = findOptimalIndex( tableRight, colPosArr );

            AbstractJoin join;
            Table tableRef;
            Table tableRefAux;

            // TODO on app startup, I can "query" all the query builders.
            //  or store the query plan after the first execution so startup time is faster
            //      take care to leave the holes whenever the substitution is necessary

            // now we have to decide the physical operators
            if(leftIndexOptional.isPresent() && rightIndexOptional.isPresent()){
                // does this case necessarily lead to symmetric HashJoin? https://cs.uwaterloo.ca/~david/cs448/
                final AbstractIndex<IKey> leftTableIndex = leftIndexOptional.get();
                final AbstractIndex<IKey> rightTableIndex = rightIndexOptional.get();
                if(leftTableIndex.size() >= rightTableIndex.size()){
                    join = new HashJoin( rightTableIndex, leftTableIndex );
                    tableRef = tableRight;
                    tableRefAux = tableLeft;
                } else {
                    join = new HashJoin( leftTableIndex, rightTableIndex );
                    tableRef = tableLeft;
                    tableRefAux = tableRight;
                }

            } else if( leftIndexOptional.isPresent() ){ // indexed nested loop join on inner table. the outer probes its rows
                tableRef = tableRight;
                tableRefAux = tableLeft;
                join = new IndexedNestedLoopJoin( tableRight.getPrimaryKeyIndex(), leftIndexOptional.get() );
            } else if( rightIndexOptional.isPresent() ) {
                tableRef = tableLeft;
                tableRefAux = tableRight;
                join = new IndexedNestedLoopJoin( tableLeft.getPrimaryKeyIndex(), rightIndexOptional.get() );
            } else { // nested loop join
                // ideally this is pushed upstream to benefit from pruning performed by downstream nodes
                if(tableLeft.getPrimaryKeyIndex().size() >= tableRight.getPrimaryKeyIndex().size()){
                    tableRef = tableRight;
                    tableRefAux = tableLeft;
                    join = new NestedLoopJoin(tableRight.getPrimaryKeyIndex(), tableLeft.getPrimaryKeyIndex());
                } else {
                    tableRef = tableLeft;
                    tableRefAux = tableRight;
                    join = new NestedLoopJoin(tableLeft.getPrimaryKeyIndex(), tableRight.getPrimaryKeyIndex());
                }

            }

            this.addJoinToRespectiveTableInOrder(tableRef, join, joinsPerTable);
            this.addToAuxJoinsPerTable( tableRefAux, join, joinsPerTable, auxJoinsPerTable );

        }

        // TODO optimization. the same table can appear in several joins.
        //      in this sense, which join + filter should be executed first to give the best prune of records?
        //      yes, but without the selectivity info there is nothing we can do. I can store the selectivity info
        //      on index creation, but for that I need to have tuples already stored...

        Table tableRef;
        // Merging the filters with the join
        // The heuristic now is just to apply the filter as early as possible. later we can revisit
        // remove from map after applying the additional filters
        // TODO the indexes chosen so far may not be optimal given that, by considering the filters,
        //      we may find better indexes that would make the join much faster
        //      a first approach is simply deciding now an index and later
        //      when applying the filters, we check whether there are better indexes

        for( final Map.Entry<Table,List<WherePredicate>> tableFilter : filtersForJoinGroupedByTable.entrySet() ){

            // if find in joins per table, add filter to the first join and remove the table from the aux
            tableRef = tableFilter.getKey();

            List<AbstractJoin> joins = joinsPerTable.getOrDefault( tableRef, null );
            if(joins != null) {
                // we always have at least one
                joins.get(0).setFilterInner( buildFilterInfo( tableFilter.getValue() ) );
                // should remove it from the aux too in case there is some entry there, e.g., is the outer
                // although the method addAux safeguards that, there may be an interleaving with addJoinTo...
                auxJoinsPerTable.remove( tableRef );
                // remove it from here in case this table is outer in other join
                filtersForJoinGroupedByTable.remove( tableRef );
                // iterator.remove();

                // if the outer table is in aux, then apply the filter, so it guarantees "optimality" (not guaranteed)
                // the joins are in order here. take advantage to iterate over the outer indexes
                for( AbstractJoin join : joins ){
                    List<WherePredicate> list = filtersForJoinGroupedByTable.get( join.getOuterIndex().getTable() );
                    if( list != null ){
                        join.setFilterOuter( buildFilterInfo( list ) );
                        filtersForJoinGroupedByTable.remove( join.getOuterIndex().getTable() );
                    }
                }

            } else {
                // otherwise, find in the aux, add to the first join (suboptimal) and remove the table from aux
                joins = auxJoinsPerTable.getOrDefault( tableRef, null );
                joins.get(0).setFilterOuter( buildFilterInfo( tableFilter.getValue() ) );
                auxJoinsPerTable.remove( tableRef );
                filtersForJoinGroupedByTable.remove( tableRef );
                //iterator.remove();
            }

        }

        PlanNode planNodeToReturn = null;

        // left deep, most cases will fall in this category
        if(treeType == QueryTreeTypeEnum.LEFT_DEEP){
            // here I need to iterate over the joins defined earlier to build the plan tree for the joins
            PlanNode previous = null;
            PlanNode leaf = null;
            // possible optimization
            // if(joinsPerTable.size() == 1){}

            final List<PlanNode> headers = new ArrayList<>(joinsPerTable.size());

            for(final Map.Entry<Table,List<AbstractJoin>> joinEntry : joinsPerTable.entrySet()){
                List<AbstractJoin> joins = joinEntry.getValue();
                for( final AbstractJoin join : joins ) {
                    final PlanNode node = new PlanNode(join);
                    node.left = previous;
                    if(previous != null){
                        previous.father = node;
                    } else {
                        leaf = node;
                    }
                    previous = node;
                }

                headers.add( previous );
                previous = null;

            }

            // == 1 means no cartesian product
            if(headers.size() == 1){
                planNodeToReturn = leaf;

                // now build the projection
                PlanNode projection = buildProjection( queryTree );
                projection.left = planNodeToReturn;
                planNodeToReturn.father = projection;


            } else {
                // cartesian product
            }

        } else{
            // TODO finish BUSHY TREE later

        }


        // a scan for each table
        // the scan strategy only applies for those tables not involved in any join
        // TODO all predicates with the same column should be a single filter
        // TODO build filter checks where the number of parameters are known a priori and can
        //      take better advantage of the runtime, i.e., avoid loops over the filters
        //      E.g., eval2(predicate1, values2, predicate2, values2) ...
        //      each one of the tables here lead to a cartesian product operation
        //      if there are any join, so it should be pushed upstream to avoid high cost
        for( final Map.Entry<Table, List<WherePredicate>> entry : whereClauseGroupedByTableNotInAnyJoin.entrySet() ){

            final List<WherePredicate> wherePredicates = entry.getValue();
            final Table currTable = entry.getKey();

            int[] filterColumns = wherePredicates.stream().mapToInt( WherePredicate::getColumnPosition ).toArray();

            // I need the index information to decide which operator, e.g., index scan
            final Optional<AbstractIndex<IKey>> optimalIndexOptional = findOptimalIndex( currTable, filterColumns );
            if(optimalIndexOptional.isPresent()){
                AbstractIndex<IKey> optimalIndex = optimalIndexOptional.get();
                if( optimalIndex.getType() == IndexDataStructureEnum.HASH) {
                    // what columns are not subject for hash probing?
                    int[] indexColumns = optimalIndex.getColumns();

                    Object[] indexParams = new Object[indexColumns.length];

                    // build a new list of where predicates without the columns involved in the index,
                    // since the index will only lead to the interested values
                    List<WherePredicate> wherePredicatesNoIndex = new ArrayList<>();
                    // FIXME naive n^2 ... indexColumns can be a map
                    boolean found = false;
                    int idxParamPos = 0;
                    for(WherePredicate wherePredicate : wherePredicates){
                        for(int indexColumn : indexColumns){
                            if( indexColumn == wherePredicate.columnReference.columnPosition ){
                                indexParams[idxParamPos] = wherePredicate.value;
                                idxParamPos++;
                                found = true;
                                break;
                            }
                        }
                        if(!found) wherePredicatesNoIndex.add( wherePredicate );
                        found = false;
                    }

                    // build hash probe
                    // the key to probe in the hash index
                    IKey probeKey;
                    if (indexColumns.length > 1) {
                        probeKey = new CompositeKey(indexParams);
                    } else {
                        probeKey = new SimpleKey(indexParams[0]);
                    }

                    IndexScan indexScan;
                    if(wherePredicatesNoIndex.size() == 0){
                        indexScan = new IndexScan( optimalIndex, probeKey );
                    } else {
                        final FilterInfo filterInfo = buildFilterInfo(wherePredicates);
                        indexScan = new IndexScan(  optimalIndex, probeKey, filterInfo );
                    }

                    planNodeToReturn = new PlanNode(indexScan);
                    PlanNode projection = buildProjection( queryTree );
                    projection.left = planNodeToReturn;
                    planNodeToReturn.father = projection;

                } else {
                    final FilterInfo filterInfo = buildFilterInfo( wherePredicates );
                    final SequentialScan seqScan = new SequentialScan(optimalIndex, filterInfo);
                }
            } else {
                final FilterInfo filterInfo = buildFilterInfo( wherePredicates );
                final SequentialScan seqScan = new SequentialScan(currTable.getPrimaryKeyIndex(), filterInfo);
            }

        }

        // TODO think about parallel seq scan,
        // SequentialScan sequentialScan = new SequentialScan( filterList, table);

        return planNodeToReturn;
    }

    private PlanNode buildProjection(final QueryTree queryTree){

        PlanNode projectionPlanNode = null;

        // case where we need to convert to a class type
        if(queryTree.returnType != null){
            Projector projector = new Projector( queryTree.returnType, queryTree.projections );
            projectionPlanNode = new PlanNode();
            projectionPlanNode.consumer = projector;
        } else {
            // simply a return containing rows
            // TODO later
        }
        return projectionPlanNode;
    }

    private FilterInfo buildFilterInfo( final List<WherePredicate> wherePredicates ) throws FilterBuilderException {

        final int size = wherePredicates.size();
        IFilter<?>[] filters = new IFilter<?>[size];
        int[] filterColumns = new int[size];
        Collection<IdentifiableNode<Object>> filterParams = new LinkedList<>();

        int i = 0;
        for(final WherePredicate w : wherePredicates){

            boolean nullableExpression = w.expression == ExpressionTypeEnum.IS_NOT_NULL
                    || w.expression == ExpressionTypeEnum.IS_NULL;

            filters[ i ] = FilterBuilder.build( w );
            filterColumns[ i ] = w.columnReference.columnPosition;
            if( !nullableExpression ){
                filterParams.add( new IdentifiableNode<>( i, w.value ) );
            }

            i++;
        }

        return new FilterInfo(filters, filterColumns, filterParams);

    }

    /**
     * some tables might not be the "left" table in the join operation, thus filter loop
     * would miss these table filters. we need to maintain a separate map (auxJoinsPerTable)
     * with the "right" tables
     * when they appear as "left" (joinsPerTable) they are removed from the auxJoinsPerTable
     * FIXME right now I am not sorting the list as in addJoinToRespectiveTableInOrder (M)
     *       in order to reuse M, I would need to modularize the condition of the inner/outer table
     *       to a separate method, which could be defined by a boolean parameter (e.g., inner == false)
     * @param tableRef
     */
    private void addToAuxJoinsPerTable(final Table tableRef,
                                       final AbstractJoin join,
                                       final Map<Table,List<AbstractJoin>> joinsPerTable,
                                       final Map<Table,List<AbstractJoin>> auxJoinsPerTable){
        if(joinsPerTable.get( tableRef ) != null) {
            return;
        }
        List<AbstractJoin> auxJoins = auxJoinsPerTable.getOrDefault(tableRef,new ArrayList<>());
        auxJoins.add( join );
        auxJoinsPerTable.put( tableRef, auxJoins );
    }

    /** the shortest the table, lower the degree of the join operation in the query tree
     * thus, store a map of plan nodes keyed by table
     * the plan node with the largest probability of pruning (considering index type + filters + size of table)
     * more records earlier in the query are pushed downstream in the physical query plan
     * ordered list of participating tables. the first one is the smallest table and so on.
     * better selectivity rate leads to a deeper node
     * Consider that the base table is always the left one in the join
     * So I just need to check to order the plan nodes according to the size of the right table
     * TODO binary search
     * Priority order of sorting conditions: type of join, selectivity (we are lacking this right now), size of (right) table
     * @param tableRef
     * @param join
     * @param joinsPerTable
     */
    private void addJoinToRespectiveTableInOrder( final Table tableRef, final AbstractJoin join, Map<Table,List<AbstractJoin>> joinsPerTable ){

        List<AbstractJoin> joins = joinsPerTable.getOrDefault(tableRef,new ArrayList<>());

        if(joins.size() == 0){
            joins.add(join);
            joinsPerTable.put( tableRef, joins );
            return;
        }
        Iterator<AbstractJoin> it = joins.iterator();
        int posToInsert = 0;
        AbstractJoin next = it.next();
        boolean hasNext = true;
        while (hasNext &&
                (
                  (
                    ( next.getType() == HASH && (join.getType() == INDEX_NESTED_LOOP || join.getType() == NESTED_LOOP ) )
                    || // TODO this may not be true for all joins. e.g., some index nested loop might take longer than a nested loop join
                    ( next.getType() == INDEX_NESTED_LOOP && (join.getType() == NESTED_LOOP ) )
                  )
                  ||
                  ( join.getOuterIndex().size() > next.getOuterIndex().size() )
                )
            )
        {
            if(it.hasNext()){
                next = it.next();
                posToInsert++;
            } else {
                hasNext = false;
            }

        }
        joins.add(posToInsert, join);
    }

    /**
     *  Here we are relying on the fact that the developer has declared the columns
     *  in the where clause matching the actual index column order definition
     */
    private Optional<AbstractIndex<IKey>> findOptimalIndex(final Table table, final int[] filterColumns){

        // all combinations... TODO this can be built in the analyzer
        final List<int[]> combinations = getAllPossibleColumnCombinations(filterColumns);

        // build map of hash code
        Map<Integer,int[]> hashCodeMap = new HashMap<>(combinations.size());

        for(final int[] arr : combinations){
            if( arr.length == 1 ) {
                hashCodeMap.put(arr[0], arr);
            } else {
                hashCodeMap.put(Arrays.hashCode(arr), arr);
            }
        }
        
        // for each index, check if the column set matches
        float bestSelectivity = Float.MAX_VALUE;
        AbstractIndex<IKey> optimalIndex = null;

        final List<AbstractIndex<IKey>> indexes = new ArrayList<>(table.getIndexes().size() + 1 );
        indexes.addAll( table.getIndexes() );
        indexes.add(0, table.getPrimaryKeyIndex() );

        // the selectivity may be unknown, we should use simple heuristic here to decide what is the best index
        // the heuristic is simply the number of columns an index covers
        // of course this can be misleading, since the selectivity can be poor, leading to scan the entire table anyway...
        // TODO we could insert a selectivity collector in the sequential scan so later this data can be used here
        for( final AbstractIndex<IKey> index : indexes ){

            /*
             * in case selectivity is not present,
             * then we use size the type of the column, e.g., int > long > float > double > String
             *  https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
             */
            // for this algorithm I am only considering exact matches, so range indexes >,<,>=,=< are not included for now
            int[] columns = hashCodeMap.getOrDefault( index.hashCode(), null );
            if( columns != null ){

                // optimistic belief that the number of columns would lead to higher pruning of records
                float heuristicSelectivity = table.getPrimaryKeyIndex().size() / columns.length;

                // try next index then
                if(heuristicSelectivity > bestSelectivity) continue;

                // if tiebreak, hash index type is chosen as the tiebreaker criterion
                if(heuristicSelectivity == bestSelectivity
                        && optimalIndex.getType() == IndexDataStructureEnum.TREE
                        && index.getType() == IndexDataStructureEnum.HASH){
                    // do not need to check whether optimalIndex != null since totalSize
                    // can never be equals to FLOAT.MAX_VALUE
                    optimalIndex = index;
                    continue;
                }

                // heuristicSelectivity < bestSelectivity
                optimalIndex = index;
                bestSelectivity = heuristicSelectivity;

            }

        }

        return Optional.of(optimalIndex);

    }

    private List<int[]> getCombinationsFor2SizeColumnList( final int[] filterColumns ){
        int[] arr0 = { filterColumns[0] };
        int[] arr1 = { filterColumns[1] };
        int[] arr2 = { filterColumns[0], filterColumns[1] };
        return Arrays.asList( arr0, arr1, arr2 );
    }

    private List<int[]> getCombinationsFor3SizeColumnList( final int[] filterColumns ){
        int[] arr0 = { filterColumns[0] };
        int[] arr1 = { filterColumns[1] };
        int[] arr2 = { filterColumns[2] };
        int[] arr3 = { filterColumns[0], filterColumns[1], filterColumns[2] };
        int[] arr4 = { filterColumns[0], filterColumns[1] };
        int[] arr5 = { filterColumns[0], filterColumns[2] };
        int[] arr6 = { filterColumns[1], filterColumns[2] };
        return Arrays.asList( arr0, arr1, arr2, arr3, arr4, arr5, arr6 );
    }

    // TODO later get metadata to know whether such a column has an index, so it can be pruned from this search
    // TODO a column can appear more than once. make it sure it appears only once
    public List<int[]> getAllPossibleColumnCombinations( final int[] filterColumns ){

        // in case only one condition for join and single filter
        if(filterColumns.length == 1) return Arrays.asList(filterColumns);

        if(filterColumns.length == 2) return getCombinationsFor2SizeColumnList(filterColumns);

        if(filterColumns.length == 3) return getCombinationsFor3SizeColumnList(filterColumns);

        final int length = filterColumns.length;

        // not exact number, an approximation!
        int totalComb = (int) Math.pow( length, 2 );

        List<int[]> listRef = new ArrayList<>( totalComb );

        for(int i = 0; i < length - 1; i++){
            int[] base = Arrays.copyOfRange( filterColumns, i, i+1 );
            listRef.add ( base );
            for(int j = i+1; j < length; j++ ) {
                listRef.add(Arrays.copyOfRange(filterColumns, i, j + 1));

                // now get all possibilities without this j
                if (j < length - 1) {
                    int k = j + 1;
                    int[] aux1 = {filterColumns[i], filterColumns[k]};
                    listRef.add(aux1);

                    // if there are more elements, then I form a new array including my i and k
                    // i.e., is k the last element? if not, then I have to perform the next operation
                    if (k < length - 1) {
                        int[] aux2 = Arrays.copyOfRange(filterColumns, k, length);
                        int[] aux3 = Arrays.copyOf(base, base.length + aux2.length);
                        System.arraycopy(aux2, 0, aux3, base.length, aux2.length);
                        listRef.add(aux3);
                    }

                }

            }

        }

        listRef.add( Arrays.copyOfRange( filterColumns, length - 1, length ) );
        return listRef;

    }

}
