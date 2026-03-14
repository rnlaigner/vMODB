package dk.ku.di.dms.vms.tpcc.order.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.definition.key.composite.PairCompositeKey;
import dk.ku.di.dms.vms.tpcc.order.entities.NewOrder;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@Repository
public interface INewOrderRepository extends IRepository<NewOrder.NewOrderId, NewOrder> {

    static Map<PairCompositeKey, TreeMap<NewOrder.NewOrderId, NewOrder>> NEW_ORDERS = new HashMap<>();

    @Query("select * from new_orders where no_d_id = :no_d_id and no_w_id = :no_w_id order by no_o_id asc limit 1")
    NewOrder getFirstNewOrder(int no_d_id, int no_w_id);

    static NewOrder getFirstInTree(int no_d_id, int no_w_id) {
        var treeMap = NEW_ORDERS.get( PairCompositeKey.of(no_d_id, no_w_id) );
        synchronized (treeMap) {
            return treeMap.pollFirstEntry().getValue();
        }
    }

    static void insertToTree(NewOrder newOrder) {
        var treeMap = NEW_ORDERS.get( PairCompositeKey.of(newOrder.no_d_id, newOrder.no_w_id) );
        synchronized (treeMap) {
            treeMap.put(newOrder.getId(), newOrder);
        }
    }

}