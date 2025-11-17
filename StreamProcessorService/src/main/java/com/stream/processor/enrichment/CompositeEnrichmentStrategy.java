package com.stream.processor.enrichment;

import com.stream.processor.Model.SensorEvent;
import lombok.RequiredArgsConstructor;

import java.util.List;

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
