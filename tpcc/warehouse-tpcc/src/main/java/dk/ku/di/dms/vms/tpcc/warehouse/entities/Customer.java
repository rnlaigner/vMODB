package dk.ku.di.dms.vms.tpcc.warehouse.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
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
@VmsTable(name="customer")
@IdClass(Customer.CustomerId.class)
public class Customer implements IEntity<Customer.CustomerId> {

    public static class CustomerId implements Serializable {
        public int c_id;
        public int c_d_id;
        public int c_w_id;

        public CustomerId(){}

        public CustomerId(int c_id, int c_d_id, int c_w_id) {
            this.c_id = c_id;
            this.c_d_id = c_d_id;
            this.c_w_id = c_w_id;
        }
    }

    @Id
    public int c_id;

    // @VmsIndex(name = "c_last_idx")
    @Id
    @VmsForeignKey(table=District.class, column = "d_id")
    public int c_d_id;

    @Id
    @VmsForeignKey(table=District.class, column = "d_w_id")
    @VmsIndex(name = "c_last_idx")
    public int c_w_id;

    @Column
    public String c_first;

    @Column
    public String c_middle;

    @VmsIndex(name = "c_last_idx")
    @Column
    public String c_last;

    @Column
    public Date c_since;

    @Column
    public String c_credit;

    @Column
    public float c_credit_lim;

    @Column
    public float c_discount;

    @Column
    public float c_balance;

    @Column
    public float c_ytd_payment;

    @Column
    public int c_payment_cnt;

    @Column
    public int c_delivery_cnt;

    @Column
    public String c_data;

    public Customer(){}

    public Customer(int c_id, int c_d_id, int c_w_id,
                    String c_first, String c_middle, String c_last,
                    String  c_street_1, String c_street_2, String c_city, String c_state, String c_zip, String c_phone,
                    Date c_since, String c_credit, float c_credit_lim, float c_discount, float c_balance, int c_ytd_payment,
                    int c_payment_cnt, int c_delivery_cnt, String c_data) {
        this.c_id = c_id;
        this.c_d_id = c_d_id;
        this.c_w_id = c_w_id;
        this.c_discount = c_discount;
        this.c_first = c_first;
        this.c_middle = c_middle;
        this.c_last = c_last;
        this.c_since = c_since;
        this.c_credit = c_credit;
        this.c_credit_lim = c_credit_lim;
        this.c_balance = c_balance;
        this.c_ytd_payment = c_ytd_payment;
        this.c_payment_cnt = c_payment_cnt;
        this.c_delivery_cnt = c_delivery_cnt;
        this.c_data = c_data;
    }

    @Override
    public String toString() {
        return "{"
                + "\"c_id\":" + c_id
                + ",\"c_w_id\":" + c_w_id
                + ",\"c_d_id\":" + c_d_id
                + ",\"c_first\":\"" + c_first + "\""
                + ",\"c_middle\":\"" + c_middle + "\""
                + ",\"c_last\":\"" + c_last + "\""
                + ",\"c_since\":\"" + c_since.getTime() + "\""
                + ",\"c_credit\":\"" + c_credit + "\""
                + ",\"c_credit_lim\":" + c_credit_lim
                + ",\"c_discount\":" + c_discount
                + ",\"c_balance\":" + c_balance
                + ",\"c_ytd_payment\":" + c_ytd_payment
                + ",\"c_payment_cnt\":" + c_payment_cnt
                + ",\"c_delivery_cnt\":" + c_delivery_cnt
                + ",\"c_data\":\"" + c_data + "\""
                + "}";
    }
}