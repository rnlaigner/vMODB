package dk.ku.di.dms.vms.tpcc.common.events.stock_level;

import dk.ku.di.dms.vms.modb.api.annotations.Event;
import dk.ku.di.dms.vms.tpcc.common.etc.WareDistId;

@Event
public final class StockLevelWareOut {

    public int w_id;
    public int d_id;
    public int threshold;
    public int next_o_id;

    @SuppressWarnings("unused")
    public StockLevelWareOut(){}

    public StockLevelWareOut(int w_id, int d_id, int threshold, int next_o_id) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.threshold = threshold;
        this.next_o_id = next_o_id;
    }

    @SuppressWarnings("unused")
    public WareDistId getId(){
        return new WareDistId(this.w_id, 0);
    }

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":" + w_id
                + ",\"d_id\":" + d_id
                + ",\"threshold\":" + threshold
                + ",\"next_o_id\":" + next_o_id
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof StockLevelWareOut that){
            if (this.w_id != that.w_id) return false;
            if (this.d_id != that.d_id) return false;
            if (this.threshold != that.threshold) return false;
            return this.next_o_id == that.next_o_id;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.w_id;
        result = 31 * result + this.d_id;
        result = 31 * result + this.threshold;
        result = 31 * result + this.next_o_id;
        return result;
    }

}