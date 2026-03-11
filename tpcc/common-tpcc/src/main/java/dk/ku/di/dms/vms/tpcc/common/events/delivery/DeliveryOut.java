package dk.ku.di.dms.vms.tpcc.common.events.delivery;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

import java.util.Arrays;
import java.util.Objects;

@Event
public final class DeliveryOut {

    public int w_id;
    public int[] customerIds;
    public float[] amounts;

    @SuppressWarnings("unused")
    public DeliveryOut(){}

    public DeliveryOut(int w_id, int[] customerIds, float[] amounts) {
        this.w_id = w_id;
        this.customerIds = customerIds;
        this.amounts = amounts;
    }

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":" + w_id
                + ",\"customerIds\":" + Arrays.toString(customerIds)
                + ",\"amounts\":" + Arrays.toString(amounts)
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DeliveryOut that){
            if (this.w_id != that.w_id) return false;
            if (this.customerIds.length != that.customerIds.length) return false;
            for (int i = 0; i < this.customerIds.length; i++){
                if (!Objects.equals(this.customerIds[i], that.customerIds[i])) return false;
            }
            if (this.amounts.length != that.amounts.length) return false;
            for (int i = 0; i < this.amounts.length; i++){
                if (!Objects.equals(this.amounts[i], that.amounts[i])) return false;
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.w_id;
        result = 31 * result + Arrays.hashCode(this.customerIds);
        result = 31 * result + Arrays.hashCode(this.amounts);
        return result;
    }

}