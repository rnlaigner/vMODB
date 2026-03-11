package dk.ku.di.dms.vms.tpcc.common.events.delivery;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class DeliveryIn {

    public int w_id;
    public int carrier_id;

    @SuppressWarnings("unused")
    public DeliveryIn(){}

    public DeliveryIn(int w_id, int carrier_id) {
        this.w_id = w_id;
        this.carrier_id = carrier_id;
    }

    /**
     * Exact order id is not known beforehand, so partition is warehouse ID
     */
    @SuppressWarnings("unused")
    public int getId(){
        return this.w_id;
    }

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":" + w_id
                + ",\"carrier_id\":" + carrier_id
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DeliveryIn that){
            return (this.w_id == that.w_id && this.carrier_id == that.carrier_id);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.w_id;
        result = 31 * result + this.carrier_id;
        return result;
    }

}