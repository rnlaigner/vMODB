package dk.ku.di.dms.vms.modb.transaction.multiversion.pk;

public interface IPrimaryKeyGenerator<T extends Number> {

    T next();

}
