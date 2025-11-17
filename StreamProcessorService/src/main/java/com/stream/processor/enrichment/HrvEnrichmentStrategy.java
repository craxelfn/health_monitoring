package com.stream.processor.enrichment;

import com.stream.processor.Model.SensorEvent;
import org.springframework.stereotype.Component;


@Component
public class HrvEnrichmentStrategy implements EnrichmentStrategy {

    @Override
    public SensorEvent enrich(SensorEvent event) {
        if (event.getHrv() != null || event.getHeartRate() == null) {
            return event;
        }
        double hrv = calculateHRV(event.getHeartRate());
        return event.toBuilder().hrv(hrv).build();
    }

    private double calculateHRV(double heartRate) {
        double rrInterval = 60000.0 / heartRate;
        double hrv = rrInterval * 0.1;
        return Math.max(20, Math.min(200, hrv));
    }
}
