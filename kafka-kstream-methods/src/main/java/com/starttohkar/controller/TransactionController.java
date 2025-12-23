package com.starttohkar.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starttohkar.events.Transaction;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.InputStream;
import java.util.List;

@RestController
@RequestMapping("/api/transactions")
public class TransactionController {

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    public TransactionController(KafkaTemplate<String, Transaction> kafkaTemplate){
        this.kafkaTemplate=kafkaTemplate;
    }

//    @PostMapping
//    public String sendTransaction() throws Exception{
//        for (int i=0;i<50;i++){
//            String transactionId = "txn-" + System.currentTimeMillis()+"-"+i;
//            double amount = 8000 + new Random().nextDouble() * (11000 - 8000);
//
//            Transaction txn = new Transaction(transactionId,"USER_"+ i, amount, LocalDateTime.now().toString());
//
//           // String txnJson = mapper.writeValueAsString(txn);
//            kafkaTemplate.send("transactions-2", transactionId, txn);
//        }
//        return "Transaction Send to Kafka !!!";
//    }


    @PostMapping("/publish")
    public String publishTransaction() {
        List<Transaction> transactions = readTransactionsFromResource();

        for (Transaction txn : transactions) {
            kafkaTemplate.send("transactions", txn.transactionId(), txn);
        }
        return "✅ Published " + transactions.size() + " transactions to Kafka!";
    }


    private List<Transaction> readTransactionsFromResource() {
        // open the resource stream for /transactions.json from the classpath
        try (InputStream is = getClass().getResourceAsStream("/transactions.json")) {
            return mapper.readValue(is, new TypeReference<List<Transaction>>() {
            });
        } catch (Exception e) {
            // wrap any parsing errors and rethrow as a runtime exception
            throw new RuntimeException("Failed to parse transactions.json", e);
        }
    }
}
