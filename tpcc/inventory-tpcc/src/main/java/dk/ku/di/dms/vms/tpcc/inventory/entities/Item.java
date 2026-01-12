package dk.ku.di.dms.vms.tpcc.inventory.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
@VmsTable(name="item")
public final class Item implements IEntity<Integer> {

    @Id
    public int i_id;

    @Column
    public int i_im_id;

    @Column
    public String i_name;

    @Column
    public float i_price;

    @Column
    public String i_data;

    @SuppressWarnings("unused")
    public Item(){}

    @SuppressWarnings("unused")
    public Item(int i_id, int i_im_id, float i_price, String i_name, String i_data) {
        this.i_id = i_id;
        this.i_im_id = i_im_id;
        this.i_name = i_name;
        this.i_price = i_price;
        this.i_data = i_data;
    }

    @Override
    public String toString() {
        return "{"
                + "\"i_id\":" + i_id
                + ",\"i_im_id\":" + i_im_id
                + ",\"i_name\":\"" + i_name + "\""
                + ",\"i_price\":" + i_price
                + ",\"i_data\":\"" + i_data + "\""
                + "}";
    }
}