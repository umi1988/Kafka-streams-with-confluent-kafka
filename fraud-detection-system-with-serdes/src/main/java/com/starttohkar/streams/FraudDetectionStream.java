package com.starttohkar.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.starttohkar.events.Transaction;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
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

    @Bean
    public KStream<String, Transaction> fraudDetectStream(StreamsBuilder builder) {
        // This approach is not allowed in production code as we don't have much control in the code.
        // if we need to perform custom serialization and de-serialization, then we can't do.
        var transactionSerdes = new JacksonJsonSerde<>(Transaction.class);

        KStream<String, Transaction> stream =
                builder.stream("transactions-1", Consumed.with(Serdes.String(), transactionSerdes));
        // functional style code
        stream
                .filter((key, tx)-> tx.amount()>10000)
                .peek((key, tx) -> log.warn("⚠️ FRAUD ALERT for {}", tx))
                .to("fraud-alerts-1", Produced.with(Serdes.String(),transactionSerdes));

        return stream;

    }
}

