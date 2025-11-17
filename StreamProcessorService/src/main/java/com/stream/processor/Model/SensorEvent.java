package com.stream.processor.Model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;


@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = {"messageId", "patientId"})
@JsonIgnoreProperties(ignoreUnknown = true)
public class SensorEvent {

    @JsonProperty("message_id")
    private String messageId;

    @JsonProperty("patient_id")
    private String patientId;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("heart_rate")
    private Double heartRate;

    @JsonProperty("blood_pressure_systolic")
    private Integer bloodPressureSystolic;

    @JsonProperty("blood_pressure_diastolic")
    private Integer bloodPressureDiastolic;

    @JsonProperty("temperature")
    private Double temperature;

    @JsonProperty("oxygen_saturation")
    private Double oxygenSaturation;

    @JsonProperty("respiratory_rate")
    private Integer respiratoryRate;

    @JsonProperty("activity_level")
    private String activityLevel;

    @JsonProperty("hrv")
    private Double hrv;

    @JsonProperty("bmi")
    private Double bmi;

    @JsonProperty("processed_at")
    private Long processedAt;
}
