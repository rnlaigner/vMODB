package dk.ku.di.dms.vms.modb.query.analyzer;

import dk.ku.di.dms.vms.modb.definition.ColumnReference;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.GroupByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.OrderByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;

import java.util.*;

/**
 *  Logical query plan tree
  */
public final class QueryTree {

    public Optional<Boolean> distinct;

    // projection
    public final List<ColumnReference> projections;

    // store all tables included in this query, including those not participating in any join
    public final Map<String, Table> tables;

    // join operations
    public final List<JoinPredicate> joinPredicates;

    // predicates
    public final List<WherePredicate> wherePredicates;

    // aggregate operations
    public final List<GroupByPredicate> groupByProjections;

    public final List<ColumnReference> groupByColumns;

    public final List<OrderByPredicate> orderByPredicates;

    public Optional<Integer> limit;

    public QueryTree(List<WherePredicate> wherePredicates){
        this.wherePredicates = wherePredicates;
        this.projections = Collections.emptyList();
        this.tables = Collections.emptyMap();
        this.joinPredicates = Collections.emptyList();
        this.groupByProjections = Collections.emptyList();
        this.groupByColumns = Collections.emptyList();
        this.orderByPredicates = Collections.emptyList();
        this.distinct = Optional.empty();
        this.limit = Optional.empty();
    }

    public QueryTree() {
        this.projections = new ArrayList<>(3);
        this.tables = new HashMap<>(2);
        this.joinPredicates = new ArrayList<>();
        this.wherePredicates = new ArrayList<>(3);
        this.groupByProjections = new ArrayList<>();
        this.groupByColumns = new ArrayList<>();
        this.orderByPredicates = new ArrayList<>();
    }

    /**
      * This method allows the index selection to avoid a great number of combinations
      * on selecting an index. An index is stored with columns ordered.
      * So when we pass the columns of table involved in a query to the planner,
      * this goes already in order.
      * @param wherePredicate the predicate to add
      */
    public void addWhereClauseSortedByColumnIndex( WherePredicate wherePredicate ){
        int size = this.wherePredicates.size();
        if(size == 0){
            this.wherePredicates.add(wherePredicate);
            return;
        }

        if(size == 1){
            if(wherePredicate.columnReference.columnPosition >
                    this.wherePredicates.getFirst().columnReference.columnPosition) {
                this.wherePredicates.add(1, wherePredicate);
            } else {
                this.wherePredicates.addFirst(wherePredicate);
            }
            return;
        }

        // if size >= 2 then use binary search
        int pos = getPosition(wherePredicate, size);
        this.wherePredicates.add(pos, wherePredicate);
    }

    private int getPosition(WherePredicate wherePredicate, int size) {
        int pos;
        int start = 1, end = size;
        do {
            pos = ((end + start) / 2);
            if(this.wherePredicates.get(pos-1).columnReference.columnPosition >
                    wherePredicate.columnReference.columnPosition){
                if(end == pos){
                    end = end - 1;
                } else {
                    // it guarantees "end" is within bounds
                    end = pos;
                }
            } else {
                if(pos == start){
                    start = pos + 1;
                } else {
                    start = pos;// it guarantees "start" is within bounds
                }
            }
        } while(start != end);
        pos = start - 1;
        if(this.wherePredicates.get(pos).columnReference.columnPosition <
                wherePredicate.columnReference.columnPosition){
            pos++;
        }
        return pos;
    }

    /**
     * Simple select if no joins, grouping, and sort are present
     * @return whether it is a simple scan
     */
    public boolean isSimpleScan(){
        return this.isSingleTable() && this.joinPredicates.isEmpty() && this.groupByProjections.isEmpty() && this.orderByPredicates.isEmpty();
    }

    public boolean isSimpleScanWithOrder(){
        return this.isSingleTable() && this.joinPredicates.isEmpty() && this.groupByProjections.isEmpty() && !this.orderByPredicates.isEmpty();
    }

    public boolean isSimpleAggregate(){
        return this.isSingleTable() && this.groupByProjections.size() == 1 && this.joinPredicates.isEmpty() && this.orderByPredicates.isEmpty();
    }

    public boolean isSimpleJoin(){
        return this.joinPredicates.size() == 1 && this.groupByProjections.isEmpty() && this.orderByPredicates.isEmpty();
    }

    /**
     * Also if this.groupByProjections.size() > 1
     */
    public boolean hasMultipleJoins(){
        return this.joinPredicates.size() > 1;
    }

    public boolean isSingleTable(){
        return this.tables.size() == 1;
    }

    public boolean hasMultipleAggregates() {
        return this.isSingleTable() && this.groupByProjections.size() > 1 && this.joinPredicates.isEmpty() && this.orderByPredicates.isEmpty();
    }

}
