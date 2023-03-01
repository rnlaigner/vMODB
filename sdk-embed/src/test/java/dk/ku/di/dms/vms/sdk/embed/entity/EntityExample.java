package dk.ku.di.dms.vms.sdk.embed.entity;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
@VmsTable(name="entity_example")
public class EntityExample implements IEntity<Long> {

    @Id
    public long key;

    @Column
    public int n;

    public EntityExample(long key, int n) {
        this.key = key;
        this.n = n;
    }
}
