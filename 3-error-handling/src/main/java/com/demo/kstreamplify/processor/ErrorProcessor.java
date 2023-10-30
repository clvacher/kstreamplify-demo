package com.demo.kstreamplify.processor;

import com.demo.kstreamplify.model.ParcelEnrichmentProcessingResult;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;


/**
 * Generic error processor.
 */
public class ErrorProcessor implements FixedKeyProcessor<String, ParcelEnrichmentProcessingResult, String> {
    private FixedKeyProcessorContext<String, String> context;

    /**
     * Init context.
     *
     * @param context the context to init
     */

    @Override
    public void init(FixedKeyProcessorContext<String, String> context) {
        this.context = context;
    }

    /**
     * Process the error.
     *
     * @param fixedKeyRecord the record to process an error
     */
    @Override
    public void process(FixedKeyRecord<String, ParcelEnrichmentProcessingResult> fixedKeyRecord) {

        RecordMetadata recordMetadata = context.recordMetadata().get();

        String error =
                fixedKeyRecord.value().getException().getMessage() +
                        " topic: " + recordMetadata.topic() +
                        " partition: " + recordMetadata.partition() +
                        " offset: " + recordMetadata.offset();

        context.forward(fixedKeyRecord.withValue(error));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // may close resource opened in init
    }
}
