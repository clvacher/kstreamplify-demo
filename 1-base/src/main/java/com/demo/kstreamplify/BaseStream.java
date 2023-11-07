package com.demo.kstreamplify;

import com.demo.kstreamplify.avro.Parcel;
import com.demo.kstreamplify.properties.KafkaProperties;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import static com.demo.kstreamplify.constants.Constants.TOPIC_DATA_IN;
import static com.demo.kstreamplify.constants.Constants.TOPIC_ENRICH_OUT;
import static com.demo.kstreamplify.constants.Constants.TOPIC_REF;

@Component
@Getter
public class BaseStream implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(BaseStream.class);

    private final KafkaProperties kafkaProperties;

    private KafkaStreams streams;

    public BaseStream(KafkaProperties kafkaProperties) {
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

        SpecificAvroSerde<Parcel> parcelSpecificAvroSerde = new SpecificAvroSerde<>();
        parcelSpecificAvroSerde.configure(this.kafkaProperties.getProperties(), false);

        final StreamsBuilder builder = new StreamsBuilder();

        // Stream the input topic
        KStream<String, Parcel> streamDataIn = builder.stream(
                TOPIC_DATA_IN,
                Consumed.with(Serdes.String(),  parcelSpecificAvroSerde)
        );

        // GlobalKTable for the referential data
        GlobalKTable<String, String> tableRefData = builder.globalTable(
                TOPIC_REF,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("GLOBAL_REF_STORE")
                        .withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
        );

        // Join the stream with the referential data
        streamDataIn.join(
                        tableRefData,
                        (leftKey, Parcel) -> Parcel.getItem(),
                        Pair::of
                )
                // append the itemNumber to the value
                .mapValues(BaseStream::enrichWithReferential)
                // send the result to the output topic
                .to(TOPIC_ENRICH_OUT, Produced.with(Serdes.String(), parcelSpecificAvroSerde));

        return builder.build();

    }

    private static Parcel enrichWithReferential(Pair<Parcel, String> joinResultPair) {
        // Extract areaCode from referential side
        String areaCode = joinResultPair.getRight().substring(7, 10);

        // Extract Parcel from stream side
        Parcel parcel = joinResultPair.getLeft();

        // Set areaCode in Parcel
        parcel.setAreaCode(areaCode);

        // Return Parcel
        return parcel;
    }

}

