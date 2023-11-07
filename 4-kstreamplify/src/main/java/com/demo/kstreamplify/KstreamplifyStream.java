package com.demo.kstreamplify;

import com.demo.kstreamplify.avro.Parcel;
import com.michelin.kstreamplify.error.ProcessingResult;
import com.michelin.kstreamplify.error.TopologyErrorHandler;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.stereotype.Component;
import com.demo.kstreamplify.contants.*;

@Component
public class KstreamplifyStream extends KafkaStreamsStarter {

    @Override
    public String dlqTopic() {
        return PackageEnricherTopics.packageEnricherDlqTopic().toString();
    }

    @Override
    public void topology(StreamsBuilder streamsBuilder) {
        // Stream input topic
        var inputStream = PackageEnricherTopics.inputTopic().stream(streamsBuilder);

        // GlobalKTable for the referential data
        var globalKTable = PackageEnricherTopics.referentialTopic().globalTable(streamsBuilder, "GLOBAL_REF_STORE");

        // Join the stream with the referential data
        var joinedStream = inputStream.join(
                globalKTable,
                (k,v) -> v.getItem(),
                Pair::of);

        // Enrich the package with the referential data and catch errors to dlq
        var enrichedStream = TopologyErrorHandler.catchErrors(joinedStream.mapValues(this::enrichWithReferential));

        // Write the enriched package to the output topic
        PackageEnricherTopics.outputTopic().produce(enrichedStream);
    }

    private ProcessingResult<Parcel, Parcel> enrichWithReferential(Pair<Parcel,String> packageAndReferential){
        try{
            // Extract the Parcel from stream side
            var parcel = packageAndReferential.getLeft();

            // Extract the areaCode from referential side and put it in the Parcel
            parcel.setAreaCode(packageAndReferential.getRight().substring(7,10));

            // Return the successful record
            return ProcessingResult.success(parcel);
        }
        catch(Exception e){
            // Return the failed record
            return ProcessingResult.fail(e, packageAndReferential.getLeft(), "Issue occurred while trying to enrich package with referential");
        }
    }
}
