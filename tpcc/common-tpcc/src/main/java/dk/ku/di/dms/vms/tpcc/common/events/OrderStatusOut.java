package dk.ku.di.dms.vms.tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class OrderStatusOut {

    public int w_id;
    public int d_id;
    public int c_id;

    @SuppressWarnings("unused")
    public OrderStatusOut(){}

    @SuppressWarnings("unused")
    public OrderStatusOut(int w_id, int d_id, int c_id) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.c_id = c_id;
    }

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":" + w_id
                + ",\"d_id\":" + d_id
                + ",\"c_id\":" + c_id
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof OrderStatusOut that) {
            if (this.w_id != that.w_id) return false;
            if (this.d_id != that.d_id) return false;
            return this.c_id == that.c_id;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.w_id;
        result = 31 * result + this.d_id;
        result = 31 * result + this.c_id;
        return result;
    }

}