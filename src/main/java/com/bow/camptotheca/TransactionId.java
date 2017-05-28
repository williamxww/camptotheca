package com.bow.camptotheca;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId implements Serializable {

    private static final long serialVersionUID = 1L;

    private static AtomicLong counter = new AtomicLong(0);

    private final long myId;

    public TransactionId() {
        myId = counter.getAndIncrement();
    }

    public long getId() {
        return myId;
    }

    @Override
    public boolean equals(Object tid) {
        return ((TransactionId) tid).myId == myId;
    }

    @Override
    public int hashCode() {
        return (int) myId;
    }
}
