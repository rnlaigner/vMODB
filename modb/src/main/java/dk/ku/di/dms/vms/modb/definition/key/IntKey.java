package dk.ku.di.dms.vms.modb.definition.key;

public class IntKey implements IKey {

    public int value;

    // private constructor
    private IntKey() {}

    public static IntKey of() {
        return new IntKey();
    }

    public static IntKey of(int value){
        IntKey key = new IntKey();
        key.value = value;
        return key;
    }

    public IntKey newValue(int newValue){
        this.value = newValue;
        return this;
    }

    @Override
    public int hashCode(){
        return this.value;
    }

    @Override
    public boolean equals(Object object){
        return object instanceof IntKey other && other.value == this.value;
    }

    @Override
    public String toString() {
        return "{"
                + "\"value\":" + value
                + "}";
    }
}
