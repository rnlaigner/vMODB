package dk.ku.di.dms.vms.modb.transaction.multiversion.pk;

public final class PrimaryKeyGeneratorBuilder {

    @SuppressWarnings("unchecked")
    public static <T extends Number> IPrimaryKeyGenerator<T> build(Class<?> type){
        if (type == Long.class) {
            return (IPrimaryKeyGenerator<T>) new LongPrimaryKeyGenerator();
        }
        if (type == Integer.class) {
           return (IPrimaryKeyGenerator<T>) new LongPrimaryKeyGenerator();
        }
        throw new IllegalStateException("Cannot provide a PK generator for the type "+type.getSimpleName());
    }

}
