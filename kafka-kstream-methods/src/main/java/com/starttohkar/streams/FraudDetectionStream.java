package com.starttohkar.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.starttohkar.events.Transaction;
import com.starttohkar.serdes.TransactionSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

@Configuration
@EnableKafkaStreams
public class FraudDetectionStream {

    Logger log =  LoggerFactory.getLogger(FraudDetectionStream.class);
    //create bean
    //-> read the topic
    //-> process filter
    //-> write to dest

    //    filter(),
    //    filterNot(),
    //    map(),
    //    mapValues(),
    //    flatMap(),
    //    flatMapValues(),
    //    branch(),
    //    groupBy(),
    //    aggregate(),
    //    count()

    @Bean
    public KStream<String, Transaction> fraudDetectStream(StreamsBuilder builder) {

        KStream<String, Transaction> stream =
                builder.stream("transactions-2", Consumed.with(Serdes.String(), new TransactionSerde()));


//        stream
//                .filter((key, tx) -> tx.amount() > 25000)
//                .peek((key, tx) -> log.warn("⚠️ FRAUD ALERT for {}", tx));

//        stream
//                .filterNot((key, tx) -> tx.amount() < 10000)
//                .peek((key, tx) -> log.warn("⚠️ normal transaction for {}", tx));


//        stream.map((key, tx) ->
//                KeyValue.pair(tx.userId(), "user spent amount : " + tx.amount())
//        ).peek((key, value) ->
//                log.info("User Transaction Summary: Key: {}, Value: {}", key, value)
//        );
//
//        stream.mapValues(tx -> "Transaction of ₹" + tx.amount() + " by user " + tx.userId())
//                .peek((key, tx) ->
//                        log.info("User Transaction Summary Value Only: Key: {}, Value: {}", key, tx)
//                );

//        stream.flatMap((key,tx)->{
//            List<KeyValue<String, Item>> result=new ArrayList<>();
//            for(Item item:tx.items()){
//                result.add(KeyValue.pair(tx.transactionId(), item));
//            }
//            return result;
//        }).peek((key, item) ->
//                log.info("flatMap ---- Item Purchased: Transaction ID: {}, Item: {}", key, item));
//
//
//        stream.flatMapValues(Transaction::items)
//                .peek((key, item) ->
//                        log.info("flatMapValues --- Item Purchased Value Only: Transaction ID: {}, Item: {}", key, item));

//        KStream<String, Transaction>[] branch = stream
//                .branch(
//                        (key, tx) -> tx.type().equalsIgnoreCase("debit"),
//                        (key, tx) -> tx.type().equalsIgnoreCase("credit")
//                );
//
//        branch[0].peek((key, tx) ->
//                log.info("Debit Transaction: Key: {}, Transaction: {}", key, tx)
//        ).to("debit_transactions", Produced.with(Serdes.String(), new TransactionSerde()));
//
//        branch[1].peek((key, tx) ->
//                log.info("Credit Transaction: Key: {}, Transaction: {}", key, tx)
//        ).to("credit_transactions", Produced.with(Serdes.String(), new TransactionSerde()));


//        stream
//                .groupBy((key, tx) -> tx.location())
//                .count()
//                .toStream()
//                .peek((loc, count) ->
//                        log.info("🌍 Location {} has {} transactions", loc, count)
//                );

//        stream.groupBy((key, tx) -> tx.userId())
//                .count(Materialized.as("user-txn-count-store"))
//                .toStream()
//                .peek((userId, count) ->
//                        log.info("👥 User {} made {} transactions", userId, count)
//                );


        stream.groupBy((key, tx) -> tx.type())
                .aggregate(
                        () -> 0.0,
                        (type, tx, currentSum) -> currentSum + tx.amount(),
                        Materialized.with(Serdes.String(), Serdes.Double())
                ).toStream()
                .peek((type, total) ->
                        log.info("CardType: {} | 💰 Running Total Amount: {}", type, total)
                );


        return stream;

    }
}

