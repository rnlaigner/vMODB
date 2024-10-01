package dk.ku.di.dms.vms.modb.transaction.internal;

/**
 * A data structure that serves the only purpose of storing historical writes of a key
 * regarding an index. Assumptions:
 * (i) It assumes there will be only one writer at every single time.
 * (ii) It also assumes new entries are appended to the head.
 * (iii) Readers never read from the head, unless that task is completed (all-or-nothing atomicity).
 * concurrently with a writer, as long as the keys being inserted, removed, and read do not
 * intersect. In other words, concurrent threads are supposed to always operate on
 * distinct partitions of the data structure.
 */
public sealed class OneWriterMultiReadersLIFO<K extends Comparable<K>,V> permits OperationSetOfKey {

    private volatile Entry<K,V> head;

    public final void put(K key, V val){
        // always insert in the front
        Entry<K,V> currFirst = this.head;
        this.head = new Entry<>(key, val, currFirst);
    }

    /**
     * Removes the head of the data structure
     */
    @SuppressWarnings("UnnecessaryLocalVariable")
    public final void poll(){
        assert this.head != null;
        var next = this.head.next;
        this.head = next;
    }

    public final Entry<K,V> peak(){
        return this.head;
    }

    /**
     * Gets the entry corresponding to the specified key; if no such entry
     * exists, returns the entry for the greatest key less than the specified
     * key; if no such entry exists, returns {@code null}.
     */
    public final Entry<K,V> floorEntry(K key) {
        if(this.head == null) return null;
        Entry<K,V> curr = this.head;
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
     * Gets the entry for the highest key equal or below the specified
     * key; if no such entry exists, returns {@code null}.
     * In other words, gets the immediate successor of key.
     */
    public final Entry<K,V> getHigherEntryUpToKey(K key) {
        if(this.head == null) return null;
        // is parameter key already higher than the highest entry? if so, just return it
        if(key.compareTo(this.head.key) >= 0) return this.head;
        Entry<K,V> next = this.head;
        Entry<K,V> curr;
        int cmp;
        do {
            curr = next;
            next = next.next;
            if(next == null) break;
            cmp = curr.key.compareTo(key);
        } while(cmp > 0); // curr node is higher than parameter key? if so, continue
        // it means no entry key is below the parameter key
        if(next == null && curr.key.compareTo(key) > 0) return null;
        return curr;
    }

    /**
     * Remove all entries below the key
     * Method is used to remove TIDs that cannot be seen anymore
     * Not safe if there are concurrent writers and the entry returned is the head
     * @param key node identifier
     */
    public final Entry<K,V> removeUpToEntry(K key){
        final Entry<K,V> entryToReturn = this.getHigherEntryUpToKey(key);
        this.removeChildren(entryToReturn);
        return entryToReturn;
    }

    public final void removeChildren(final Entry<K,V> entry){
        Entry<K,V> currFloorEntry = entry;
        Entry<K,V> auxEntry;
        while(currFloorEntry != null){
            auxEntry = currFloorEntry.next;
            // set next to null to lose reference
            currFloorEntry.next = null;
            currFloorEntry = auxEntry;
        }
    }

    public final void clear(){
        if(this.head == null) return;
        Entry<K,V> current = this.head;
        Entry<K,V> next;
        while (current != null){
            next = current.next;
            current.next = null;
            current = next;
        }
    }

    @Override
    public final String toString(){
        var current = this.head;
        if(current == null) return "";
        String lineSeparator = System.lineSeparator();
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
