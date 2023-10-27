package com.bdxio.stream;

import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import com.bdxio.stream.properties.KafkaProperties;

import java.util.Map;

import static com.bdxio.stream.constants.Constants.*;

@Component
@Getter
public class BdxIoStream implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(BdxIoStream.class);

    private final KafkaProperties kafkaProperties;

    private KafkaStreams streams;


    public BdxIoStream(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public void run(ApplicationArguments args) {

        try {
            // Create KafkaStreams instance
            Topology topology = getTopology();
            logger.info("topology: {}", topology.describe());
            streams = new KafkaStreams(topology, kafkaProperties.asProperties());

            // Start the stream
            streams.start();

            // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    streams.close();
                } catch (Exception e) {
                    logger.warn("Error while trying to close stream", e.getMessage(), e);
                }
            }));
        } catch (Exception e) {
            logger.error("Cannot start stream processor", e.getMessage(), e);
        }
    }

    /**
     * build the stream topology
     *
     * @return
     */
    public Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> streamDataIn = builder.stream(
                TOPIC_DATA_IN,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        GlobalKTable<String, String> tableRefData = builder.globalTable(
                TOPIC_REF,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("GLOBAL_REF_STORE")
                        .withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
        );

        streamDataIn.join(
                        tableRefData,
                        (leftKey, leftValue) -> leftKey,
                        Pair::of
                )
                .mapValues(pair ->
                        transformValue(pair)
                )

                .to(TOPIC_ENRICH_OUT, Produced.with(Serdes.String(), Serdes.String()));


        return builder.build();

    }

    private static String transformValue(Pair<String, String> pair) {
        return pair.getLeft() + ":::" + pair.getRight().substring(7, 10);
    }


}

