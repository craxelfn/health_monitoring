package com.stream.processor.validation;

import com.stream.processor.Model.SensorEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RequiredFieldsValidator implements ValidationRule {

    @Override
    public ValidationResult validate(SensorEvent event) {
        if (event.getPatientId() == null || event.getPatientId().isEmpty()) {
            return ValidationResult.invalid("Patient ID is required");
        }
        if (event.getMessageId() == null || event.getMessageId().isEmpty()) {
            return ValidationResult.invalid("Message ID is required");
        }
        if (event.getTimestamp() == null || event.getTimestamp().isEmpty()) {
            return ValidationResult.invalid("Timestamp is required");
        }
        if (event.getHeartRate() == null || event.getHeartRate() <= 0) {
            return ValidationResult.invalid("Valid heart rate is required");
        }
        return ValidationResult.valid();
    }
}