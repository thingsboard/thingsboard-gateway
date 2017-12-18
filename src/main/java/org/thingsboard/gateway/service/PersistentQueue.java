package org.thingsboard.gateway.service;

import org.mapdb.*;

/**
 * Created by Valerii Sosliuk on 12/17/2017.
 */
public class PersistentQueue<T> {

    private BTreeMapJava.KeySet<T> queue;
    private DB db;

    public PersistentQueue(DB db) {
        queue = (BTreeMapJava.KeySet<T>) db.treeSet("messageQueue").serializer(Serializer.JAVA).createOrOpen();
    }

    public void add(T t) {
        queue.add(t);
    }

    public T removeFirst() {
        return queue.pollFirst();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }
}
