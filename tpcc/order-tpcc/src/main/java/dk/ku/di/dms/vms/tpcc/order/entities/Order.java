package dk.ku.di.dms.vms.tpcc.order.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsIndex;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import java.io.Serializable;
import java.util.Date;

@Entity
@VmsTable(name="orders")
@IdClass(Order.OrderId.class)
public final class Order implements IEntity<Order.OrderId> {

    public static class OrderId implements Serializable {
        public int o_id;
        public int o_d_id;
        public int o_w_id;
        public OrderId(int o_id, int o_d_id, int o_w_id) {
            this.o_id = o_id;
            this.o_d_id = o_d_id;
            this.o_w_id = o_w_id;
        }
    }

    @Id
    public int o_id;

    @Id
    @VmsIndex(name = "customer_idx")
    public int o_d_id;

    @Id
    @VmsIndex(name = "customer_idx")
    public int o_w_id;

    @Column
    @VmsIndex(name = "customer_idx")
    public int o_c_id;

    @Column
    public Date o_entry_d;

    @Column
    public int o_carrier_id;

    @Column
    public int o_ol_cnt;

    @Column
    public int o_all_local;

    public Order(){}

    public Order(int o_id, int o_d_id, int o_w_id, int o_c_id, Date o_entry_d, int o_carrier_id, int o_ol_cnt, int o_all_local) {
        this.o_id = o_id;
        this.o_d_id = o_d_id;
        this.o_w_id = o_w_id;
        this.o_c_id = o_c_id;
        this.o_entry_d = o_entry_d;
        this.o_carrier_id = o_carrier_id;
        this.o_ol_cnt = o_ol_cnt;
        this.o_all_local = o_all_local;
    }

}