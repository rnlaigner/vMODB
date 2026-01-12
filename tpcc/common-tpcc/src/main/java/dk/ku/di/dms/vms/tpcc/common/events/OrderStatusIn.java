package dk.ku.di.dms.vms.tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class OrderStatusIn {

    public int w_id;
    public int d_id;
    public int c_id;
    public String c_last;
    public boolean by_name;

    @SuppressWarnings("unused")
    public OrderStatusIn(){}

    public OrderStatusIn(int w_id, int d_id, int c_id, String c_last, boolean by_name) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.c_id = c_id;
        this.c_last = c_last;
        this.by_name = by_name;
    }

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":" + w_id
                + ",\"d_id\":" + d_id
                + ",\"c_id\":" + c_id
                + ",\"c_last\":\"" + c_last + "\""
                + ",\"by_name\":" + by_name
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof OrderStatusIn that) {
            if (this.w_id != that.w_id) return false;
            if (this.d_id != that.d_id) return false;
            if (this.c_id != that.c_id) return false;
            if (!this.c_last.equals(that.c_last)) return false;
            return this.by_name == that.by_name;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.w_id;
        result = 31 * result + this.d_id;
        result = 31 * result + this.c_id;
        result = 31 * result + this.c_last.hashCode();
        result = 31 * result + (this.by_name ? 1 : 0);
        return result;
    }

}