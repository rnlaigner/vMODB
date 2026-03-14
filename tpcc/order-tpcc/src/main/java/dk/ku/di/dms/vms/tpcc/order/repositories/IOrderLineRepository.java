package dk.ku.di.dms.vms.tpcc.order.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.order.dto.OrderLineInfoDto;
import dk.ku.di.dms.vms.tpcc.order.entities.OrderLine;

import java.util.List;

@Repository
public interface IOrderLineRepository extends IRepository<OrderLine.OrderLineId, OrderLine> {

    @Query("select ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount from order_line " +
            "where ol_o_id = :o_id and ol_d_id = :c_d_id and ol_w_id = :o_w_id")
    List<OrderLineInfoDto> getOrderLinesInfo(int o_id, int c_d_id, int o_w_id);

    @Query("select * from order_line where ol_o_id = :o_id and ol_d_id = :d_id and ol_w_id = :w_id")
    List<OrderLine> getAllByOrderId(int o_id, int d_id, int w_id);

    @Query("select distinct ol_i_id from order_line where ol_o_id IN (:orderIds) and ol_d_id = :d_id and ol_w_id = :w_id")
    int[] getAllItemsByOrderIds(int[] orderIds, int d_id, int w_id);

}