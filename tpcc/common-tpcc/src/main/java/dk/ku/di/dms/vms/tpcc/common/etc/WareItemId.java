package dk.ku.di.dms.vms.tpcc.common.etc;

public final class WareItemId {

    public final int w_id;
    public final int i_id;

    public WareItemId(int w_id, int i_id) {
        this.w_id = w_id;
        this.i_id = i_id;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof WareItemId that){
            if (this.w_id != that.w_id) return false;
            return this.i_id == that.i_id;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 31 * this.w_id;
        return 31 * result + this.i_id;
    }

}