package dk.ku.di.dms.vms.tpcc.inventory.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsForeignKey;
import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.validation.constraints.PositiveOrZero;
import java.io.Serializable;
import java.util.Map;

@Entity
@VmsTable(name="stock")
@IdClass(Stock.StockId.class)
public final class Stock implements IEntity<Stock.StockId> {

    public static class StockId implements Serializable {
        public int s_i_id;
        public int s_w_id;

        public StockId(int s_i_id, int s_w_id) {
            this.s_i_id = s_i_id;
            this.s_w_id = s_w_id;
        }
    }

    @Id
    @VmsForeignKey(table=Item.class, column = "i_id")
    public int s_i_id;

    @Id
    // @VmsForeignKey(table=Warehouse.class, column = "w_id")
    public int s_w_id;

    @Column
    @PositiveOrZero
    public int s_quantity;

    @Column
    public String s_dist_01;
    @Column
    public String s_dist_02;
    @Column
    public String s_dist_03;
    @Column
    public String s_dist_04;
    @Column
    public String s_dist_05;
    @Column
    public String s_dist_06;
    @Column
    public String s_dist_07;
    @Column
    public String s_dist_08;
    @Column
    public String s_dist_09;
    @Column
    public String s_dist_10;

    @Column
    public int s_ytd;

    @Column
    public int s_order_cnt;

    @Column
    public int s_remote_cnt;

    @Column
    public String s_data;

    @SuppressWarnings("unused")
    public Stock(){}

    @SuppressWarnings("unused")
    public Stock(int s_i_id, int s_w_id, int s_quantity, Map<Integer, String> s_dist, int s_ytd, int s_order_cnt, int s_remote_cnt, String s_data) {
        this.s_i_id = s_i_id;
        this.s_w_id = s_w_id;
        this.s_quantity = s_quantity;

        // dist
        this.s_dist_01 = s_dist.get(1);
        this.s_dist_02 = s_dist.get(2);
        this.s_dist_03 = s_dist.get(3);
        this.s_dist_04 = s_dist.get(4);
        this.s_dist_05 = s_dist.get(5);
        this.s_dist_06 = s_dist.get(6);
        this.s_dist_07 = s_dist.get(7);
        this.s_dist_08 = s_dist.get(8);
        this.s_dist_09 = s_dist.get(9);
        this.s_dist_10 = s_dist.get(10);

        this.s_ytd = s_ytd;
        this.s_order_cnt = s_order_cnt;
        this.s_remote_cnt = s_remote_cnt;

        this.s_data = s_data;
    }

    public String getDistInfo(int d_id){
        switch (d_id){
            case 1 -> {
                return this.s_dist_01;
            }
            case 2 -> {
                return this.s_dist_02;
            }
            case 3 -> {
                return this.s_dist_03;
            }
            case 4 -> {
                return this.s_dist_04;
            }
            case 5 -> {
                return this.s_dist_05;
            }
            case 6 -> {
                return this.s_dist_06;
            }
            case 7 -> {
                return this.s_dist_07;
            }
            case 8 -> {
                return this.s_dist_08;
            }
            case 9 -> {
                return this.s_dist_09;
            }
            case 10 -> {
                return this.s_dist_10;
            }
            default -> throw new RuntimeException("d_id"+d_id+" unknown.");
        }
    }

    @Override
    public String toString() {
        return "{"
                + "\"s_i_id\":\"" + s_i_id + "\""
                + ",\"s_w_id\":\"" + s_w_id + "\""
                + ",\"s_quantity\":\"" + s_quantity + "\""
                + ",\"s_dist_01\":\"" + s_dist_01 + "\""
                + ",\"s_dist_02\":\"" + s_dist_02 + "\""
                + ",\"s_dist_03\":\"" + s_dist_03 + "\""
                + ",\"s_dist_04\":\"" + s_dist_04 + "\""
                + ",\"s_dist_05\":\"" + s_dist_05 + "\""
                + ",\"s_dist_06\":\"" + s_dist_06 + "\""
                + ",\"s_dist_07\":\"" + s_dist_07 + "\""
                + ",\"s_dist_08\":\"" + s_dist_08 + "\""
                + ",\"s_dist_09\":\"" + s_dist_09 + "\""
                + ",\"s_dist_10\":\"" + s_dist_10 + "\""
                + ",\"s_ytd\":\"" + s_ytd + "\""
                + ",\"s_order_cnt\":\"" + s_order_cnt + "\""
                + ",\"s_remote_cnt\":\"" + s_remote_cnt + "\""
                + ",\"s_data\":\"" + s_data + "\""
                + "}";
    }
}