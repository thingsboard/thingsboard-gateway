package org.thingsboard.gateway.service;

import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.mapdb.*;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Created by Valerii Sosliuk on 12/17/2017.
 */
public class PersistentQueue<T,I> {

    private BTreeMapJava.KeySet<T> queue;

    private ConcurrentHashMap<T, Consumer<I>> successCallbacks;
    private ConcurrentHashMap<T, Consumer<Throwable>> failureCallbacks;

    private DB db;

    public PersistentQueue(DB db) {
        queue = (BTreeMapJava.KeySet<T>) db.treeSet("messageQueue").serializer(Serializer.JAVA).createOrOpen();
        successCallbacks = new ConcurrentHashMap<>();
        failureCallbacks = new ConcurrentHashMap<>();
    }

    public void add(T t) {
        queue.add(t);
    }

    public void add(T t, Consumer<I> successCallback, Consumer<Throwable> failureCallback) {
        queue.add(t);
        successCallbacks.put(t, successCallback);
        failureCallbacks.put(t, failureCallback);
    }

    public T removeFirst() {
        return queue.pollFirst();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public Optional<Consumer<I>> getSuccessCallback(MqttMessageWrapper messageWrapper) {
        return Optional.of(successCallbacks.get(messageWrapper));
    }

    public Optional<Consumer<Throwable>> getFailureCallback(MqttMessageWrapper messageWrapper) {
        return Optional.of(failureCallbacks.get(messageWrapper));
    }

    public void removeCallbacks(T t) {
        successCallbacks.remove(t);
        failureCallbacks.remove(t);
    }
}
