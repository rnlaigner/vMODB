package dk.ku.di.dms.vms.modb.definition.key.composite;

import dk.ku.di.dms.vms.modb.definition.key.IKey;

public final class TripleCompositeKey extends BaseComposite implements IKey {

    private final Object value0;
    private final Object value1;
    private final Object value2;

    public static TripleCompositeKey of(Object value0, Object value1, Object value2){
        return new TripleCompositeKey(value0, value1, value2);
    }

    private TripleCompositeKey(Object value0, Object value1, Object value2) {
        super(hashCode(value0, value1, value2));
        this.value0 = value0;
        this.value1 = value1;
        this.value2 = value2;
    }

    private static int hashCode(Object value0, Object value1, Object value2) {
        int h = 31 + value0.hashCode();
        h = 31 * h + value1.hashCode();
        return 31 * h + value2.hashCode();
    }

    @Override
    public boolean equals(Object key){
        return key instanceof TripleCompositeKey tripleCompositeKey &&
                this.value0.equals(tripleCompositeKey.value0) &&
                this.value1.equals(tripleCompositeKey.value1) &&
                this.value2.equals(tripleCompositeKey.value2);
    }

    @Override
    public String toString() {
        return "{"
                + "\"value0\":" + this.value0
                + ",\"value1\":" + this.value1
                + ",\"value2\":" + this.value2
                + "}";
    }

    @Override
    public int size(){
        return 3;
    }

}
