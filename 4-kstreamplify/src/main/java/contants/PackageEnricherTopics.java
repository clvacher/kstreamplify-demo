package contants;

import com.demo.kstreamplify.avro.Parcel;
import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.utils.SerdesUtils;
import com.michelin.kstreamplify.utils.TopicWithSerde;
import org.apache.kafka.common.serialization.Serdes;

public final class PackageEnricherTopics {
    private PackageEnricherTopics() {}

    public static TopicWithSerde<String, Parcel> inputTopic() {
        return new TopicWithSerde<>("INPUT_DATA", Serdes.String(), SerdesUtils.getSerdesForValue());
    }

    public static TopicWithSerde<String, String> referentialTopic() {
        return new TopicWithSerde<>("REFERENTIAL_DATA", Serdes.String(), Serdes.String());
    }

    public static TopicWithSerde<String, Parcel> outputTopic() {
        return new TopicWithSerde<>("OUTPUT_DATA", Serdes.String(), SerdesUtils.getSerdesForValue());
    }

    public static TopicWithSerde<String, KafkaError> packageEnricherDlqTopic() {
        return new TopicWithSerde<>("DLQ", Serdes.String(), SerdesUtils.getSerdesForValue());
    }
}
