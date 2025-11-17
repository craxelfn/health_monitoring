package com.stream.processor.normalization;


import com.stream.processor.Model.SensorEvent;

@FunctionalInterface
public interface NormalizationStrategy {
    SensorEvent normalize(SensorEvent event);
}
