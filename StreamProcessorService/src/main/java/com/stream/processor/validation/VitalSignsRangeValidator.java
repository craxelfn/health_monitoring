package com.stream.processor.validation;

import com.stream.processor.Model.SensorEvent;
import org.springframework.stereotype.Component;

@Component
public class VitalSignsRangeValidator implements ValidationRule  {

    private static final double MIN_HEART_RATE = 30.0;
    private static final double MAX_HEART_RATE = 250.0;
    private static final double MIN_OXYGEN_SATURATION = 70.0;
    private static final double MAX_OXYGEN_SATURATION = 100.0;
    private static final double MIN_TEMPERATURE = 35.0;
    private static final double MAX_TEMPERATURE = 42.0;

    public ValidationResult validate(SensorEvent event) {
        ValidationResult result   = ValidationResult.valid();

        if (event.getHeartRate() != null) {
            if (event.getHeartRate() < MIN_HEART_RATE || event.getHeartRate() > MAX_HEART_RATE) {
                result = result.combine(ValidationResult.invalid(
                        String.format("Heart rate out of range: %.1f", event.getHeartRate())
                ));
            }
        }

        if (event.getOxygenSaturation() != null) {
            if (event.getOxygenSaturation() < MIN_OXYGEN_SATURATION ||
                    event.getOxygenSaturation() > MAX_OXYGEN_SATURATION) {
                result = result.combine(ValidationResult.invalid(
                        String.format("Oxygen saturation out of range: %.1f", event.getOxygenSaturation())
                ));
            }
        }

        if (event.getTemperature() != null) {
            if (event.getTemperature() < MIN_TEMPERATURE ||
                    event.getTemperature() > MAX_TEMPERATURE) {
                result = result.combine(ValidationResult.invalid(
                        String.format("Temperature out of range: %.1f", event.getTemperature())
                ));
            }
        }
        return result;
    }
}
