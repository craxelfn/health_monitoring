package com.stream.processor.enrichment;


import com.stream.processor.Model.SensorEvent;

@FunctionalInterface
public interface EnrichmentStrategy {

    SensorEvent enrich(SensorEvent event);
}
