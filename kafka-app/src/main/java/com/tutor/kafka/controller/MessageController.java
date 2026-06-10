package com.tutor.kafka.controller;

import com.tutor.kafka.model.UserMessage;
import com.tutor.kafka.producer.MessageProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessageProducer messageProducer;

    @PostMapping
    public ResponseEntity<UserMessage> handleMessage(@RequestBody UserMessage payload) {
        payload.setId(UUID.randomUUID().toString());
        payload.setTimestamp(Instant.now().toString());
        
        log.info("Received HTTP POST request: {}", payload.getMessage());
        
        messageProducer.sendMessage(payload);
        
        return ResponseEntity.ok(payload);
    }
}
