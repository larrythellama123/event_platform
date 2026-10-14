package com.rideplatform.core.kafka;

import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rideplatform.core.model.TripEvent;

@Component
public class KafkaProducer {
    private KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(null);
    private final ObjectMapper mapper =  new ObjectMapper(); 
    public KafkaProducer(KafkaTemplate<String, String> KT){
        kafkaTemplate = KT;
    }
    public KafkaProducer(){}
    public void sendKafka(TripEvent TE, String topicName){
        try {
            String eventString = mapper.writeValueAsString(TE);
            
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, eventString);
            
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Sent message=[" + eventString + 
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
