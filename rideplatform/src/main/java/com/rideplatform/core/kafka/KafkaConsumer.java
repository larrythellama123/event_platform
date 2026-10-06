package com.rideplatform.core.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rideplatform.core.model.Driver;
import com.rideplatform.core.model.TripEvent;
import com.rideplatform.core.service.TripService;

@Component
public class KafkaConsumer {

    private TripService tripService = new TripService();
    private final ObjectMapper mapper = new ObjectMapper();

    public void TripEventConsumer(TripService tripService){
        this.tripService = tripService;
    }

    @KafkaListener(topics = "events", groupId = "core-service")
    public void listen_rides(String payload) {
        try {
            TripEvent TE = mapper.readValue(payload, TripEvent.class);
            tripService.handle(TE);
        } catch (Exception e) {
            System.err.println("Skipping bad event: " + e.getMessage());
        }
    }

    @KafkaListener(topics = "drivers", groupId = "core-service")
    public void listen_drivers(String payload) {
        try {
            Driver D = mapper.readValue(payload, Driver.class);
            tripService.addDriver(D);
        } catch (Exception e) {
            System.err.println("Skipping bad event: " + e.getMessage());
        }
    }
}