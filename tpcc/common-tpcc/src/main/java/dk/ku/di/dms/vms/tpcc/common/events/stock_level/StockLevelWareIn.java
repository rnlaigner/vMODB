package dk.ku.di.dms.vms.tpcc.common.events.stock_level;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class StockLevelWareIn {

    public int w_id;
    public int d_id;

    @SuppressWarnings("unused")
    public StockLevelWareIn(){}

    public StockLevelWareIn(int w_id, int d_id) {
        this.w_id = w_id;
        this.d_id = d_id;
    }

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":" + w_id
                + ",\"d_id\":" + d_id
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof StockLevelWareIn that){
            if (this.w_id != that.w_id) return false;
            return this.d_id == that.d_id;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.w_id;
        result = 31 * result + this.d_id;
        return result;
    }

}