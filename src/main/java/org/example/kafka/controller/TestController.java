package org.example.kafka.controller;

import lombok.RequiredArgsConstructor;
import org.example.kafka.testKafka.TestProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class TestController {

    private final TestProducer testProducer;

    @GetMapping("/test")
    public String send() {
        testProducer.send();
        return "OK";
    }
}
