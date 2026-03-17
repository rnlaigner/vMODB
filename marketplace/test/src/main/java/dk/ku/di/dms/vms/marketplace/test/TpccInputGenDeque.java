package dk.ku.di.dms.vms.marketplace.test;

import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;

import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;

public class TpccInputGenDeque implements Deque<TransactionInput> {

    private static final String defaultPayload = String.valueOf(0);

    private static final TransactionInput INPUT = new TransactionInput("new_order", new TransactionInput.Event("new-order-ware-in", defaultPayload));

    @Override
    public void addFirst(TransactionInput transactionInput) {

    }

    @Override
    public void addLast(TransactionInput transactionInput) {

    }

    @Override
    public boolean offerFirst(TransactionInput transactionInput) {
        return false;
    }

    @Override
    public boolean offerLast(TransactionInput transactionInput) {
        return false;
    }

    @Override
    public TransactionInput removeFirst() {
        return null;
    }

    @Override
    public TransactionInput removeLast() {
        return null;
    }

    @Override
    public TransactionInput pollFirst() {
        return null;
    }

    @Override
    public TransactionInput pollLast() {
        return null;
    }

    @Override
    public TransactionInput getFirst() {
        return null;
    }

    @Override
    public TransactionInput getLast() {
        return null;
    }

    @Override
    public TransactionInput peekFirst() {
        return null;
    }

    @Override
    public TransactionInput peekLast() {
        return null;
    }

    @Override
    public boolean removeFirstOccurrence(Object o) {
        return false;
    }

    @Override
    public boolean removeLastOccurrence(Object o) {
        return false;
    }

    @Override
    public boolean add(TransactionInput transactionInput) {
        return false;
    }

    @Override
    public boolean offer(TransactionInput transactionInput) {
        return false;
    }

    @Override
    public TransactionInput remove() {
        return null;
    }

    @Override
    public TransactionInput poll() {
        return INPUT;
    }

    @Override
    public TransactionInput element() {
        return null;
    }

    @Override
    public TransactionInput peek() {
        return null;
    }

    @Override
    public boolean addAll(Collection<? extends TransactionInput> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public void push(TransactionInput transactionInput) {

    }

    @Override
    public TransactionInput pop() {
        return null;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Iterator<TransactionInput> iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public Iterator<TransactionInput> descendingIterator() {
        return null;
    }
}
