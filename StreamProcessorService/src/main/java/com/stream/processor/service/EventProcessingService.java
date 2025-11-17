package com.stream.processor.service;


import com.stream.processor.Model.SensorEvent;
import com.stream.processor.enrichment.EnrichmentStrategy;
import com.stream.processor.normalization.NormalizationStrategy;
import com.stream.processor.serialization.EventSerializer;
import com.stream.processor.serialization.SerializationException;
import com.stream.processor.validation.ValidationResult;
import com.stream.processor.validation.ValidationRule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventProcessingService {

    private final EventSerializer serializer;
    private final ValidationRule validator;
    private final NormalizationStrategy normalizer;
    private final EnrichmentStrategy enricher;


    public Optional<String> process(String jsonEvent){
        try {
            SensorEvent event = serializer.deserialize(jsonEvent);

            ValidationResult validationResult = validator.validate(event);
            if (!validationResult.isValid()) {
                log.warn("Validation failed for event {}: {}",
                        event.getMessageId(),
                        String.join(", ", validationResult.getErrors()));
                return Optional.empty();
            }

            SensorEvent processed = enricher.enrich(normalizer.normalize(event));

            String result = serializer.serialize(processed) ;
            log.debug("Successfully processed event: {}", event.getMessageId());
            return Optional.of(result);
        } catch (SerializationException e){
            log.error("Serialization error: {}", e.getMessage(), e);
            return Optional.empty();
        } catch (Exception e) {
            log.error("Processing error for payload: {}", jsonEvent, e);
            return Optional.empty();
        }
    }
}
