package dk.ku.di.dms.vms.modb.query.execution.operators.minmax;

import dk.ku.di.dms.vms.modb.definition.key.IKey;

/**
 * A key that stores the hash key used for maintained the group
 * used in the group by
 * as well as the original PK this value refers to
 */
public final class GroupByKey implements IKey {

    private final int hashKey;
    private final IKey pk;

    public GroupByKey(int hashKey, IKey pk) {
        this.hashKey = hashKey;
        this.pk = pk;
    }

    @Override
    public int hashCode() {
        return this.hashKey;
    }

    @Override
    public boolean equals(Object key){
        return this.hashKey == key.hashCode();
    }

    public IKey getPk() {
        return pk;
    }

    @Override
    public int compareTo(IKey o) {
        return pk.compareTo(o);
    }
}
