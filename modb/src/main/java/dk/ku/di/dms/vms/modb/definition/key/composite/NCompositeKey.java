package dk.ku.di.dms.vms.modb.definition.key.composite;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;

import java.util.Arrays;

/**
 * A sequence of values that serves both for identifying a
 * unique row (e.g., as PK) or a unique index entry.
 * The hash code is the hash of the array composed by all
 * values involved in this composition.
 */
public final class NCompositeKey extends BaseComposite implements IKey, IIndexKey {

    private final Object[] values;

    public static NCompositeKey of(Object[] values){
        return new NCompositeKey(values);
    }

    private NCompositeKey(Object[] values) {
        super(hashCode(values));
        this.values = values;
    }

    public static int hashCode(Object[] a) {
        int result = 1;
        for (Object element : a)
            result = 31 * result + element.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object key){
        return key instanceof NCompositeKey compositeKey &&
            Arrays.equals(this.values, compositeKey.values);
    }

    @Override
    public String toString() {
        return "{"
                + "\"values\":" + Arrays.toString(this.values)
                + "}";
    }

    @Override
    public int size(){
        return this.values.length;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public int compareTo(IKey key) {
        if (key instanceof NCompositeKey o){
            for(int i = 0; i < this.values.length; i++) {
                int result = ((Comparable)this.values[i]).compareTo(o.values[i]);
                if(result != 0){
                    return result;
                }
            }
        }
        return 0;
    }

}
