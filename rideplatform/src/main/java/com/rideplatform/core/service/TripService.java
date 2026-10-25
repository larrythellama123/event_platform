package com.rideplatform.core.service;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.rideplatform.core.kafka.KafkaProducer;
import com.rideplatform.core.model.Driver;
import com.rideplatform.core.model.TripEvent;
import com.rideplatform.core.repository.TripRepository;
import com.rideplatform.core.config.RedisConfig;
import com.rideplatform.core.repository.TripRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class TripService {
    RedisConnectionFactory connectionFactory = new LettuceConnectionFactory();
    private StringRedisTemplate redisTemplate;
    private KafkaProducer kafkaProducer;
    private final TripRepository tripRepo;
    ObjectMapper mapper = new ObjectMapper();

    public TripService(StringRedisTemplate redistemplate_, KafkaProducer kafkaProducer_, TripRepository tripRepo_){
        this.redisTemplate =redistemplate_;
        this.kafkaProducer = kafkaProducer_;
        this.tripRepo = tripRepo_;
    }

    
    public void handle(TripEvent event){
        System.out.println("STARTING HANDLE FUNC");
        try {
            System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(event));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        if(event.type.equals("REQUEST")){
            System.out.println("REQUEST FUNC");

            Point customerLocation = new Point(event.lng, event.lat);
            Distance radius = new Distance(2, DistanceUnit.KILOMETERS);
            Circle searchArea = new Circle(customerLocation, radius);

            GeoRadiusCommandArgs args = GeoRadiusCommandArgs.newGeoRadiusArgs()
            .includeDistance()
            .sortAscending();

            // GeoResults<GeoLocation<String>> results = redisTemplate.opsForGeo().radius("drivers:locations", searchArea, args);
            // if(results != null){
            //     GeoResult<GeoLocation<String>> firstRes = results.getContent().get(0);
            //     GeoLocation<String> location = firstRes.getContent();
            //     String driverID = location.getName().toString();
            //     event.driverId = driverID;
            //     event.type = "START";
            //     redisTemplate.opsForZSet().remove("drivers:locations", driverID);
            // }
            
            //stub
            event.type = "START";
            kafkaProducer.sendKafka(event, "start_events");
        }
        else if(event.type.equals( "START")){
            event.type = "COMPLETE";
            // addDriver(event.driverId, event.lng, event.lat);
            kafkaProducer.sendKafka(event, "complete");
            tripRepo.save(event);
        }
        
    }
    public void addDriver(Driver driver){
        redisTemplate.opsForGeo().add(
            "drivers:locations", 
            new Point(Double.parseDouble(driver.Lng), Double.parseDouble(driver.Lat)),
            driver.DriverID
        );
    }

    // public void addDriver(String driverID, String Lng, String Lat){
    //     redisTemplate.opsForGeo().add(
    //         "drivers:locations", 
    //         new Point(Double.parseDouble(Lng), Double.parseDouble(Lat)),
    //         driverID
    //     );
    // }

}
