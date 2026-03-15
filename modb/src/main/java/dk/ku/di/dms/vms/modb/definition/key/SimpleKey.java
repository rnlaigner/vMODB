package dk.ku.di.dms.vms.modb.definition.key;

import dk.ku.di.dms.vms.modb.definition.key.composite.NCompositeKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;

/**
 * A value that serves both for identifying a unique row (e.g., as PK) or a unique index entry.
 * In this case, the hash code is the hash of the object itself rather than the composition of values as in {@link NCompositeKey}
 */
public final class SimpleKey implements IKey, IIndexKey {

    private final int value;

    private SimpleKey(Object value) {
        this.value = value.hashCode();
    }

    private SimpleKey(int value) {
        this.value = value;
    }

    public static SimpleKey of(Object value) {
        return new SimpleKey(value);
    }

    public static SimpleKey of(int value) {
        return new SimpleKey(value);
    }

    @Override
    public int hashCode() {
        return this.value;
    }

    @Override
    public boolean equals(Object object){
        return object instanceof SimpleKey o && this.value == o.value;
    }

    @Override
    public String toString() {
        return "{" +
                "value = " + this.value +
                '}';
    }

    public int getValue() {
        return this.value;
    }

    @Override
    public int compareTo(IKey o) {
        if (o instanceof IntKey other) {
            return Integer.compare(value, other.value);
        }
        return 0;
    }
}
