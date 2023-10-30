package com.demo.kstreamplify.controller;

import com.demo.kstreamplify.LivenessLinkStream;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProbeController {

    private final LivenessLinkStream bdxioStream;

    public ProbeController(LivenessLinkStream bdxioStream) {
        this.bdxioStream = bdxioStream;
    }

    @GetMapping("/readiness")
    public ResponseEntity<String> readinessProbe() {
            if (bdxioStream.getStreams() != null && bdxioStream.getStreams().state()
                    == KafkaStreams.State.RUNNING) {
                return ResponseEntity.status(HttpStatus.OK).body("Readiness OK");
            }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("");
    }

    @GetMapping("/liveness")
    public ResponseEntity<String> livenessProbe() {
        if (bdxioStream.getStreams() != null &&
                bdxioStream.getStreams().state()
                == KafkaStreams.State.RUNNING ||
                bdxioStream.getStreams().state()
                        == KafkaStreams.State.REBALANCING) {
            return ResponseEntity.status(HttpStatus.OK).body("Liveness OK");
        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("");
    }

}
