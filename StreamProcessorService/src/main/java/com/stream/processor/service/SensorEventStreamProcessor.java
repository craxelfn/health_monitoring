package com.stream.processor.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SensorEventStreamProcessor {

    private final EventProcessingService processingService;


    @Value("${kafka.topics.input:sensor_raw}")
    private String inputTopic;

    @Value("${kafka.topics.output:processed_data}")
    private String outputTopic;


    @Bean
    public KStream<String, String> buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> rawStream = streamsBuilder.stream(inputTopic);

        KStream<String, String> processedStream = rawStream
                .peek((key, value) -> log.info("Received event for patient: {}", key))
                .mapValues(processingService::process)
                .filterNot((key, value) -> value.isEmpty())
                .mapValues(opt -> opt.orElse(null))
                .peek((key, value) -> log.info("Processed event for patient: {}", key));

        processedStream.to(outputTopic);
        return processedStream;
    }

}
