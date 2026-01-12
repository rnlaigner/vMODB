package dk.ku.di.dms.vms.tpcc.order.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import java.io.Serializable;
import java.util.Date;

@Entity
@VmsTable(name="order_line")
@IdClass(OrderLine.OrderLineId.class)
public final class OrderLine implements IEntity<OrderLine.OrderLineId> {

    public static class OrderLineId implements Serializable {
        public int ol_o_id;
        public int ol_d_id;
        public int ol_w_id;
        public int ol_number;
        public OrderLineId(int ol_o_id, int ol_d_id, int ol_w_id, int ol_number) {
            this.ol_o_id = ol_o_id;
            this.ol_d_id = ol_d_id;
            this.ol_w_id = ol_w_id;
            this.ol_number = ol_number;
        }
    }

    @Id
    @VmsForeignKey(table=Order.class, column = "o_id")
    public int ol_o_id;

    @Id
    @VmsForeignKey(table=Order.class, column = "o_d_id")
    public int ol_d_id;

    @Id
    @VmsForeignKey(table=Order.class, column = "o_w_id")
    public int ol_w_id;

    @Id
    public int ol_number;

    @Column
    public int ol_i_id;

    @Column
    public int ol_supply_w_id;

    @Column
    public Date ol_delivery_d;

    @Column
    public int ol_quantity;

    @Column
    public float ol_amount;

    @Column
    public String ol_dist_info;

    @SuppressWarnings("unused")
    public OrderLine(){}

    public OrderLine(int ol_o_id, int ol_d_id, int ol_w_id, int ol_number, int ol_i_id, int ol_supply_w_id, Date ol_delivery_d, int ol_quantity, float ol_amount, String ol_dist_info) {
        this.ol_o_id = ol_o_id;
        this.ol_d_id = ol_d_id;
        this.ol_w_id = ol_w_id;
        this.ol_number = ol_number;
        this.ol_i_id = ol_i_id;
        this.ol_supply_w_id = ol_supply_w_id;
        this.ol_delivery_d = ol_delivery_d;
        this.ol_quantity = ol_quantity;
        this.ol_amount = ol_amount;
        this.ol_dist_info = ol_dist_info;
    }
}