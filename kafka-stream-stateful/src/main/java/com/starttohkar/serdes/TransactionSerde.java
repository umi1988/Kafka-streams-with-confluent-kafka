package com.starttohkar.serdes;

import com.starttohkar.events.Transaction;
import org.apache.kafka.common.serialization.Serdes;

public class TransactionSerde  extends Serdes.WrapperSerde<Transaction> {

    public TransactionSerde(){
        super(new TransactionSerializer(),new TransactionDeserializer());
    }
}
