package com.stream.processor.configuration;

import lombok.Data;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;


@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean
    @ConfigurationProperties(prefix = "spring.kafka.streams")
    public KafkaStreamsProperties kafkaStreamsProperties() {
        return new KafkaStreamsProperties();
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig(KafkaStreamsProperties props) {
        Map<String, Object> config = new HashMap<>();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, props.getApplicationId());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, props.getNumThreads());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, props.getCommitIntervalMs());

        return new KafkaStreamsConfiguration(config);
    }

    @Data
    public static class KafkaStreamsProperties {
        private String applicationId = "stream-processor";
        private String bootstrapServers;
        private int numThreads = 2;
        private int commitIntervalMs = 1000;
    }
}