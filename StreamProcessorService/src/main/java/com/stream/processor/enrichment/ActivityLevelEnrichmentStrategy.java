package com.stream.processor.enrichment;

import com.stream.processor.Model.SensorEvent;
import org.springframework.stereotype.Component;


@Component
public class ActivityLevelEnrichmentStrategy implements EnrichmentStrategy {


    @Override
    public SensorEvent enrich(SensorEvent event) {
        if (event.getActivityLevel() != null && !event.getActivityLevel().isEmpty()) {
            return event;
        }
        return event.toBuilder().activityLevel("unknown").build();
    }
}
