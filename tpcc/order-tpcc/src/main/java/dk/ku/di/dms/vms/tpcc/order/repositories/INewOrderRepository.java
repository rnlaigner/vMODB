package dk.ku.di.dms.vms.tpcc.order.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.order.entities.NewOrder;

@Repository
public interface INewOrderRepository extends IRepository<NewOrder.NewOrderId, NewOrder> {

    @Query("select * from new_orders where no_d_id = :no_d_id and no_w_id = :no_w_id order by no_o_id asc limit 1")
    NewOrder getFirstNewOrder(int no_d_id, int no_w_id);

}