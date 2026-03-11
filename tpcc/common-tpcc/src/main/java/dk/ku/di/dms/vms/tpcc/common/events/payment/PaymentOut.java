package dk.ku.di.dms.vms.tpcc.common.events.payment;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class PaymentOut {

    public int w_id;
    public int d_id;
    public int c_id;

    public int c_w_id;
    public int c_d_id;

    public float amount;

    public String data;

    @SuppressWarnings("unused")
    public PaymentOut(){}

    public PaymentOut(int w_id, int d_id, int c_id, int c_w_id, int c_d_id, float amount, String data) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.c_id = c_id;
        this.c_w_id = c_w_id;
        this.c_d_id = c_d_id;
        this.amount = amount;
        this.data = data;
    }

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":" + w_id
                + ",\"d_id\":" + d_id
                + ",\"c_id\":" + c_id
                + ",\"c_w_id\":" + c_w_id
                + ",\"c_d_id\":" + c_d_id
                + ",\"amount\":" + amount
                + ",\"data\":\"" + data + "\""
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof PaymentOut that) {
            if (this.w_id != that.w_id) return false;
            if (this.d_id != that.d_id) return false;
            if (this.c_id != that.c_id) return false;
            if (this.c_w_id != that.c_w_id) return false;
            if (this.c_d_id != that.c_d_id) return false;
            if (this.amount != that.amount) return false;
            return this.data.equals(that.data);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.w_id;
        result = 31 * result + this.d_id;
        result = 31 * result + this.c_id;
        result = 31 * result + this.c_w_id;
        result = 31 * result + this.c_d_id;
        result = (int) (31 * result + this.amount);
        return 31 * result + this.data.hashCode();
    }

}