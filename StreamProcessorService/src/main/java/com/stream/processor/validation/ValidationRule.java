package com.stream.processor.validation;


import com.stream.processor.Model.SensorEvent;

@FunctionalInterface
public interface ValidationRule {
    ValidationResult validate(SensorEvent result);
}
