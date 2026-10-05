package com.digiteen.consumer;

import java.time.Instant;
import java.util.UUID;

public record WalletOperationEvent(UUID eventId, int schemaVersion, long transactionId,
        String transactionType, long amount, Long sourceWalletId, Long destinationWalletId,
        Instant occurredAt, UUID traceId) {

    public void validate() {
        if (eventId == null || traceId == null || occurredAt == null || schemaVersion != 1
                || transactionId <= 0 || amount <= 0 || transactionType == null) {
            throw new IllegalArgumentException("Invalid event envelope");
        }
        boolean validSides = switch (transactionType) {
            case "DEPOSIT" -> sourceWalletId == null && positive(destinationWalletId);
            case "WITHDRAWAL" -> positive(sourceWalletId) && destinationWalletId == null;
            case "TRANSFER" -> positive(sourceWalletId) && positive(destinationWalletId)
                    && !sourceWalletId.equals(destinationWalletId);
            default -> false;
        };
        if (!validSides) {
            throw new IllegalArgumentException("Invalid event type or wallet sides");
        }
    }

    private static boolean positive(Long id) {
        return id != null && id > 0;
    }
}
