package dk.ku.di.dms.vms.tpcc.proxy.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
@VmsTable(name="warehouse")
public final class Warehouse implements IEntity<Integer> {

    @Id
    public int w_id;

    @Column
    public String w_name;

    @Column
    public String w_street_1;

    @Column
    public String w_street_2;

    @Column
    public String w_city;

    @Column
    public String w_state;

    @Column
    public String w_zip;

    @Column
    public float w_tax;

    @Column
    public double w_ytd;

    public Warehouse(){}

    public Warehouse(int w_id, String w_name, String w_street_1, String w_street_2, String w_city, String w_state, String w_zip, float w_tax, double w_ytd) {
        this.w_id = w_id;
        this.w_name = w_name;
        this.w_street_1 = w_street_1;
        this.w_street_2 = w_street_2;
        this.w_city = w_city;
        this.w_state = w_state;
        this.w_zip = w_zip;
        this.w_tax = w_tax;
        this.w_ytd = w_ytd;
    }

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":" +           this.w_id
                + ",\"w_name\":\"" +        this.w_name + "\""
                + ",\"w_street_1\":\"" +    this.w_street_1 + "\""
                + ",\"w_street_2\":\"" +    this.w_street_2 + "\""
                + ",\"w_city\":\"" +        this.w_city + "\""
                + ",\"w_state\":\"" +       this.w_state + "\""
                + ",\"w_zip\":\"" +         this.w_zip + "\""
                + ",\"w_tax\":" +         this.w_tax
                + ",\"w_ytd\":" +         this.w_ytd
                + "}";
    }
}