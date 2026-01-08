package dk.ku.di.dms.vms.tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;
import dk.ku.di.dms.vms.tpcc.common.etc.WareItemId;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Event
public final class NewOrderWareOut {

    public int w_id;
    public int d_id;
    public int c_id;
    public int[] itemsIds;
    public int[] supWares;
    public int[] qty;
    public boolean allLocal;
    // from warehouse
    public double w_tax;
    public int d_next_o_id;
    public double d_tax;
    public float c_discount;

    public NewOrderWareOut(){}

    public NewOrderWareOut(int w_id, int d_id, int c_id, int[] itemsIds, int[] supWares, int[] qty, boolean allLocal, double w_tax, int d_next_o_id, double d_tax, float c_discount) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.c_id = c_id;
        this.itemsIds = itemsIds;
        this.supWares = supWares;
        this.qty = qty;
        this.allLocal = allLocal;
        this.w_tax = w_tax;
        this.d_next_o_id = d_next_o_id;
        this.d_tax = d_tax;
        this.c_discount = c_discount;
    }

    @SuppressWarnings("unused")
    public Set<Integer> getId(){
        if(this.allLocal) return Set.of(this.w_id);
        Set<Integer> set = new HashSet<>(this.supWares.length);
        for (int supWare : this.supWares) {
            set.add(supWare);
        }
        return set;
    }

    /**
     * Allows for finer-grained access to state
     */
    @SuppressWarnings("unused")
    public Set<WareItemId> getIds(){
        Set<WareItemId> set = new HashSet<>(this.itemsIds.length);
        for (int i = 0; i < this.itemsIds.length; i++) {
            set.add(new WareItemId(this.supWares[i], this.itemsIds[i]));
        }
        return set;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NewOrderWareIn that){
            if (this.w_id != that.w_id) return false;
            if (this.d_id != that.d_id) return false;
            if (this.c_id != that.c_id) return false;
            if (this.allLocal != that.allLocal) return false;
            // have to do this because remaining fields are filled as -1
            int maxSize = Math.min(this.itemsIds.length, that.itemsIds.length);
            int idx = 0;
            while(idx < maxSize){
                if(this.itemsIds[idx] != that.itemsIds[idx]){
                    return this.itemsIds[idx] == -1 || that.itemsIds[idx] == -1;
                }
                idx++;
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.w_id;
        result = 31 * result + this.d_id;
        result = 31 * result + this.c_id;
        result = 31 * result + Arrays.hashCode(this.itemsIds);
        result = 31 * result + Arrays.hashCode(this.supWares);
        result = 31 * result + Arrays.hashCode(this.qty);
        result = 31 * result + (this.allLocal ? 1 : 0);
        return result;
    }

}