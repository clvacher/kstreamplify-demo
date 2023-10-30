package com.demo.kstreamplify.model;

import com.demo.kstreamplify.avro.PackageModel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PackageEnrichmentProcessingResult {

    private PackageModel value;

    private Exception exception;
}
