package dk.ku.di.dms.vms.modb.common.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.function.Function;

public class OrderedList<K,V> extends ArrayList<V> {

    private final Function<V,K> function;
    public final Comparator<K> comparator;

    public OrderedList(Function<V,K> function, Comparator<K> comparator){
        this.function = function;
        this.comparator = comparator;
    }

    /**
     * In this particular case, the element is very likely
     * to be inserted at the start of the list, so iterating
     * makes more sense in most cases
     * @param element
     * @return boolean result
     */
    public boolean _removeLikelyHeader(V element) {

        int index = 0;
        while(index < size() && get(index).equals(element)) index++;

        if(index < size()){
            return remove(index) != null;
        } else {
            throw new RuntimeException("Cannot remove a non-existent element.");
        }

    }

    public boolean _remove(V element) {

        // some fast paths
        if( size() == 1 && element.equals( get(0) ) ) {
            return remove(0) != null;
        } else if( size() == 1 ) {
            throw new RuntimeException("Cannot remove a non-existent element.");
        }

        int index = getIndex(element);
        return remove(index) != null;
    }

    public boolean addLikelyHeader(V element){

        if(size() == 0) return super.add(element);
        if(size() == 1) {
            if(comparator.compare(function.apply( get(0) ), function.apply( element )) >= 0){
                super.add(0,element);
                return true;
            } else {
                return super.add(element);
            }
        }
        // TODO size == 2?

        int index = getIndex(element);

        try {
            super.add(index, element);
        } catch( IndexOutOfBoundsException e){
            return false;
        }
        return true;

    }

    private int getIndex(V element){
        int start = 0;
        int end = size() - 1;
        return doBinarySearchRecursion( start, end, element );
    }

    private int doBinarySearchRecursion(int start, int end, V element){

        int middle = (end - start) / 2;

        if(start >= end) return end; // return any

        if( comparator.compare(function.apply( get(middle) ), function.apply( element )) >= 0 ){
            return doBinarySearchRecursion( start, middle, element );
        }

        return doBinarySearchRecursion( middle+1, end, element );

    }

}
