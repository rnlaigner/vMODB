package dk.ku.di.dms.vms.tpcc.order.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.order.dto.OrderInfoDto;
import dk.ku.di.dms.vms.tpcc.order.entities.Order;

@Repository
public interface IOrderRepository extends IRepository<Order.OrderId, Order> {

    @Query("select o_id, o_entry_d, o_carrier_id from orders where o_id = :o_id and o_d_id = :o_d_id and o_w_id = :o_w_id and o_c_id = :o_c_id")
    OrderInfoDto getOrderInfo(int o_id, int o_d_id, int o_w_id, int o_c_id);

    @Query("select * from orders where o_c_id = :c_id and o_d_id = :o_d_id and o_w_id = :o_w_id order by o_id desc limit 1")
    Order getLastOrderByCustomerId(int c_id, int d_id, int w_id);

}