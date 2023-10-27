package com.bdxio.stream.controller;

import com.bdxio.stream.BdxIoStream;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.actuate.health.HealthComponent;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProbeController {

    private final BdxIoStream bdxioStream;

    private final HealthEndpoint healthEndpoint;

    public ProbeController(BdxIoStream bdxioStream, HealthEndpoint healthEndpoint) {
        this.bdxioStream = bdxioStream;
        this.healthEndpoint = healthEndpoint;
    }

    @GetMapping("/readiness")
    public ResponseEntity<String> readinessProbe() {
        if(Status.UP.equals(healthEndpoint.health().getStatus())){
            return ResponseEntity.status(HttpStatus.OK).body("Readiness OK");
        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("");
    }

    @GetMapping("/liveness")
    public ResponseEntity<String> livenessProbe() {
        if(Status.UP.equals(healthEndpoint.health().getStatus())){
            return ResponseEntity.status(HttpStatus.OK).body("Liveness OK");
        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("");
    }
}
