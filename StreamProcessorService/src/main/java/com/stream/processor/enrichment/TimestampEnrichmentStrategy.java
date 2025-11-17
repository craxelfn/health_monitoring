package com.stream.processor.enrichment;


import com.stream.processor.Model.SensorEvent;
import io.micrometer.observation.annotation.Observed;
import org.springframework.stereotype.Component;

@Component
public class TimestampEnrichmentStrategy implements  EnrichmentStrategy {

    @Override
    public SensorEvent enrich(SensorEvent event) {
        return event.toBuilder()
                .processedAt(System.currentTimeMillis())
                .build() ;
    }
}
