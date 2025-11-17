package com.stream.processor.configuration;


import com.stream.processor.enrichment.*;
import com.stream.processor.normalization.NormalizationStrategy;
import com.stream.processor.normalization.TimestampNormalizationStrategy;
import com.stream.processor.validation.CompositeValidator;
import com.stream.processor.validation.RequiredFieldsValidator;
import com.stream.processor.validation.ValidationRule;
import com.stream.processor.validation.VitalSignsRangeValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.List;

@Configuration
public class ProcessingConfig {

    @Bean
    @Primary
    public ValidationRule compositeValidator(
            RequiredFieldsValidator requiredFieldsValidator,
            VitalSignsRangeValidator vitalSignsRangeValidator) {

        return new CompositeValidator(List.of(
                requiredFieldsValidator,
                vitalSignsRangeValidator
        ));
    }

    @Bean
    @Primary
    public EnrichmentStrategy compositeEnricher(
            HrvEnrichmentStrategy hrvEnricher,
            ActivityLevelEnrichmentStrategy activityLevelEnricher,
            TimestampEnrichmentStrategy timestampEnricher) {

        return new CompositeEnrichmentStrategy(List.of(
                hrvEnricher,
                activityLevelEnricher,
                timestampEnricher
        ));
    }

    @Bean
    public NormalizationStrategy normalizer(TimestampNormalizationStrategy strategy) {
        return strategy;
    }
}
