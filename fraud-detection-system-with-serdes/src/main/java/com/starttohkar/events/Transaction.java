package com.starttohkar.events;

public record Transaction(
        String transactionId,
        String userId,
        double amount,
        String timestamp
) {}