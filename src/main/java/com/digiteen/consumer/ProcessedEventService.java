package com.digiteen.consumer;

import java.sql.Timestamp;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ProcessedEventService {
    private final JdbcTemplate jdbc;

    public ProcessedEventService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public boolean record(WalletOperationEvent event) {
        event.validate();
        return jdbc.update("""
                INSERT INTO processed_events (event_id, schema_version, transaction_id, type,
                    amount, source_wallet_id, destination_wallet_id, occurred_at, trace_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """, event.eventId(), event.schemaVersion(), event.transactionId(), event.transactionType(),
                event.amount(), event.sourceWalletId(), event.destinationWalletId(),
                Timestamp.from(event.occurredAt()), event.traceId()) == 1;
    }
}
