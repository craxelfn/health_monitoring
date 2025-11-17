package com.stream.processor.validation;


import com.stream.processor.Model.SensorEvent;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class CompositeValidator implements ValidationRule {

    private final List<ValidationRule> validators ;

    @Override
    public ValidationResult validate(SensorEvent event) {
        return validators.stream()
                .map(validator -> validator.validate(event))
                .reduce(ValidationResult.valid() , ValidationResult::combine);
    }
}
