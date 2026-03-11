package dk.ku.di.dms.vms.tpcc.common.events.stock_level;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class StockLevelWareIn {

    public int w_id;
    public int d_id;
    public int threshold;

    @SuppressWarnings("unused")
    public StockLevelWareIn(){}

    public StockLevelWareIn(int w_id, int d_id, int threshold) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.threshold = threshold;
    }

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":" + w_id
                + ",\"d_id\":" + d_id
                + ",\"threshold\":" + threshold
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof StockLevelWareIn that){
            if (this.w_id != that.w_id) return false;
            if (this.d_id != that.d_id) return false;
            return this.threshold == that.threshold;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.w_id;
        result = 31 * result + this.d_id;
        result = 31 * result + this.threshold;
        return result;
    }

}