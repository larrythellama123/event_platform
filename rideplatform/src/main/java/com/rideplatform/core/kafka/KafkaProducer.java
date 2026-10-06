package com.rideplatform.core.kafka;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rideplatform.core.model.TripEvent;

@Component
public class KafkaProducer {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private ObjectMapper mapper =  new ObjectMapper(); 
    public void sendKafka(TripEvent TE, String topicName){
        try {
            String eventString = mapper.writeValueAsString(TE);
            
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, eventString);
            
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Sent message=[" + TE + 
                      "] with offset=[" + result.getRecordMetadata().offset() + "]");
                } else {
                    System.err.println("Unable to send message=[" + 
                      eventString + "] due to : " + ex.getMessage());
                }
            });
            
        } catch (Exception e) {
            System.err.println("Failed to serialize TripEvent: " + e.getMessage());
        }
        
    }
}
