package com.stream.processor.normalization;

import com.stream.processor.Model.SensorEvent;
import com.stream.processor.enrichment.EnrichmentStrategy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
public class TimestampNormalizationStrategy implements NormalizationStrategy {

    @Override
    public SensorEvent normalize(SensorEvent event) {
        String timestamp = event.getTimestamp();

        if (timestamp == null || timestamp.contains("T")) {
            return event;
        }

        try {
            long epochMs = Long.parseLong(timestamp);
            String isoTimestamp = Instant.ofEpochMilli(epochMs).toString();
            return event.toBuilder().timestamp(isoTimestamp).build();
        } catch (NumberFormatException e) {
            log.warn("Could not parse timestamp: {}", timestamp);
            return event;
        }
    }
}