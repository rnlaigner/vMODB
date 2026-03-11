package dk.ku.di.dms.vms.tpcc.order.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import java.io.Serializable;

@Entity
@VmsTable(name="new_orders")
@IdClass(NewOrder.NewOrderId.class)
public final class NewOrder implements IEntity<NewOrder.NewOrderId> {

    public static class NewOrderId implements Serializable {
        public Integer no_o_id;
        public Integer no_d_id;
        public Integer no_w_id;
        public NewOrderId(Integer no_o_id, Integer no_d_id, Integer no_w_id) {
            this.no_o_id = no_o_id;
            this.no_d_id = no_d_id;
            this.no_w_id = no_w_id;
        }
    }

    @Id
    @VmsForeignKey(table=Order.class, column = "o_id")
    public int no_o_id;

    @Id
    @VmsForeignKey(table=Order.class, column = "o_d_id")
    public int no_d_id;

    @Id
    @VmsForeignKey(table=Order.class, column = "o_w_id")
    public int no_w_id;

    public NewOrder(){}

    public NewOrder(int no_o_id, int no_d_id, int no_w_id) {
        this.no_o_id = no_o_id;
        this.no_d_id = no_d_id;
        this.no_w_id = no_w_id;
    }
}