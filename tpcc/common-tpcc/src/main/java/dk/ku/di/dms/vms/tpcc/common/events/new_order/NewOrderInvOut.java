package dk.ku.di.dms.vms.tpcc.common.events.new_order;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class NewOrderInvOut {

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
    // from inventory
    public float[] prices;
    public String[] ol_dist_info;

    public NewOrderInvOut(){}

    public NewOrderInvOut(int w_id, int d_id, int c_id, int[] itemsIds, int[] supWares, int[] qty, boolean allLocal, double w_tax, int d_next_o_id, double d_tax, float c_discount, float[] prices, String[] ol_dist_info) {
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
        this.prices = prices;
        this.ol_dist_info = ol_dist_info;
    }

}