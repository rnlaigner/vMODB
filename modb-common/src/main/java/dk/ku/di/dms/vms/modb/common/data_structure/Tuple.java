package dk.ku.di.dms.vms.modb.common.data_structure;

/**
 * Basic entry to represent pair of values
 * @param <T1> value 1
 * @param <T2> value 2
 */
public class Tuple<T1,T2> {

    private final T1 t1;
    private final T2 t2;

    public Tuple(T1 t1, T2 t2){
        this.t1 = t1;
        this.t2 = t2;
    }

    public T1 getT1(){
        return t1;
    }

    public T2 getT2(){
        return t2;
    }

}
