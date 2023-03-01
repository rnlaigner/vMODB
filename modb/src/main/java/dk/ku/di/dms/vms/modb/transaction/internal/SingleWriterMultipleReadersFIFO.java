package dk.ku.di.dms.vms.modb.transaction.internal;

import dk.ku.di.dms.vms.modb.common.transaction.TransactionWrite;

import java.util.*;

/**
 * A data structure that serves the only purpose of storing historical writes of a key
 * regarding an index. Assumptions:
 * (i) It assumes there will be only one writer at every single time.
 * (ii) It also assumes new entries are appended to the head.
 * (iii) Readers never read from the head, unless that task is completed (all-or-nothing atomicity).
 * These three assumptions make it safe to call {@link #removeUpToEntry}
 * concurrently with a writer, as long as the keys being inserted, removed, and read do not
 * intersect. In other words, concurrent threads are supposed to always operate on
 * distinct partitions of the data structure.
 * @param <K> {@link dk.ku.di.dms.vms.modb.common.transaction.TransactionId}
 * @param <V> {@link TransactionWrite}
 */
public final class SingleWriterMultipleReadersFIFO<K extends Comparable<K>,V> {

    public static class Entry<K,V> implements Cloneable {
        private K key;
        private V val;

        protected Entry<K,V> next;

        public Entry(K key, V val){
            this.key = key;
            this.val = val;
        }

        public Entry(K key, V val, Entry<K,V> next){
            this.key = key;
            this.val = val;
            this.next = next;
        }

        public K key(){
            return this.key;
        }

        public V val(){
            return this.val;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Entry<K,V> clone() throws CloneNotSupportedException {
            return  (Entry<K, V>) super.clone();
            // return new Entry<>(this.key, this.val, this.next);
        }

        public void recycle(K key, V val){
            this.key = key;
            this.val = val;
        }

    }

    private transient Entry<K,V> first;

    public void put(K key, V val){
        // always insert in the front
        Entry<K,V> currFirst = this.first;
        Entry<K,V> newEntry;

        if(!removedEntries.isEmpty())
            newEntry = removedEntries.pop();
        else
            newEntry = new Entry<>(key, val);
        newEntry.next = currFirst;
        this.first = newEntry;
    }

    /**
     * Removes the head of the data structure
     */
    public void poll(){
        this.first = this.first.next;
    }

    /**
     * Gets the entry corresponding to the specified key; if no such entry
     * exists, returns the entry for the greatest key less than the specified
     * key; if no such entry exists, returns {@code null}.
     */
    public Entry<K,V> floorEntry(K key) {

        if(this.first == null) return null;

        Entry<K,V> curr = this.first;
        int cmp = curr.key.compareTo(key);
        while(cmp > 0){
            curr = curr.next;
            if(curr == null) break;
            cmp = curr.key.compareTo(key);
        }

        if(cmp <= 0) return curr;
        return null;

    }

    /**
     * Gets the entry for the lowest key higher than the specified
     * key; if no such entry exists, returns {@code null}.
     * In other words, gets the immediate successor of key.
     */
    public Entry<K,V> getHigherEntry(K key) {

        if(this.first == null) return null;

        Entry<K,V> curr = this.first;
        Entry<K,V> prev = null;
        int cmp = curr.key.compareTo(key);
        while(cmp > 0){
            prev = curr;
            curr = curr.next;
            if(curr == null) break;
            cmp = curr.key.compareTo(key);
        }

        return prev;

    }

    /**
     * Remove all entries below or equal the key
     * Method is used to remove TIDs that cannot be seen anymore
     * @param key node identifier
     */
    public void removeUpToEntry(K key){

        Entry<K,V> currFloorEntry = getHigherEntry(key);

        if(currFloorEntry == null) return;

        Entry<K,V> auxEntry;
        try {
            auxEntry = currFloorEntry.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        currFloorEntry.next = null;

        while(auxEntry != null){
            removedEntries.add( auxEntry );
            auxEntry = auxEntry.next;
        }

    }

    public void clear(){

        var current = this.first;

        if(current == null) return;

        var next = current.next;

        current.next = null;
        removedEntries.add(current);

        while (next != null){
            current = next;
            next = current.next;
            current.next = null;
            removedEntries.add(current);
        }

    }

    private final Deque<Entry<K,V>> removedEntries = new ArrayDeque<>();

    @Override
    public String toString(){

        var current = this.first;
        if(current == null) return "";

        String lineSeparator = System.getProperty("line.separator");

        StringBuilder sb = new StringBuilder();

        while (current != null){
            sb.append( current.key.toString() )
                    .append(" : ")
                    .append(current.val)
                    .append(lineSeparator);
            current = current.next;
        }

        return sb.toString();

    }

}
