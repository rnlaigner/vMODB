package dk.ku.di.dms.vms.tpcc.common.etc;

// would be nice to express a where clause
public final class WareDistId {

    public final int w_id;
    public final int d_id;

    public WareDistId(int w_id, int d_id) {
        this.w_id = w_id;
        this.d_id = d_id;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof WareDistId that){
            if (this.w_id != that.w_id) return false;
            return this.d_id == that.d_id;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 31 * this.w_id;
        return 31 * result + this.d_id;
    }

}