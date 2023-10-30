package com.demo.kstreamplify.model;

import com.demo.kstreamplify.avro.Parcel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ParcelEnrichmentProcessingResult {

    private Parcel value;

    private Exception exception;
}
