package dk.ku.di.dms.vms.tpcc.common.events.stock_level;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

import java.util.Arrays;
import java.util.Objects;

@Event
public final class StockLevelOrdOut {

    public int w_id;
    public int[] itemsIds;

    @SuppressWarnings("unused")
    public StockLevelOrdOut(){}

    public StockLevelOrdOut(int w_id, int[] itemsIds) {
        this.w_id = w_id;
        this.itemsIds = itemsIds;
    }

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":" + w_id
                + ",\"itemsIds\":" + Arrays.toString(itemsIds)
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof StockLevelOrdOut that){
            if (this.w_id != that.w_id) return false;
            if (this.itemsIds.length != that.itemsIds.length) return false;
            for (int i = 0; i < this.itemsIds.length; i++){
                if (!Objects.equals(this.itemsIds[i], that.itemsIds[i])) return false;
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.w_id;
        result = 31 * result + Arrays.hashCode(this.itemsIds);
        return result;
    }

}