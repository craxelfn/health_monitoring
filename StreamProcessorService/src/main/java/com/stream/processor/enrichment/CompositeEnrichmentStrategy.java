package com.stream.processor.enrichment;

import com.stream.processor.Model.SensorEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
@RequiredArgsConstructor
public class CompositeEnrichmentStrategy implements EnrichmentStrategy {

    private final List<EnrichmentStrategy> strategies ;

    @Override
    public SensorEvent enrich(SensorEvent event) {
        SensorEvent enriched = event ;
        for(EnrichmentStrategy strategy : strategies) {
            enriched = strategy.enrich(enriched);
        }
        return enriched;
    }
}
