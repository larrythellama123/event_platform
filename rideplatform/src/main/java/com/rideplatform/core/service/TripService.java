package com.rideplatform.core.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.rideplatform.core.model.Driver;
import com.rideplatform.core.model.TripEvent;

@Service
public class TripService {
    @Autowired 
    private StringRedisTemplate redisTemplate;

    public void handle(TripEvent event){
        
        if(event.Type == "REQUESTED"){
            Point customerLocation = new Point(Double.parseDouble(event.Lng), Double.parseDouble(event.Lat));
            Distance radius = new Distance(2, DistanceUnit.KILOMETERS);
            Circle searchArea = new Circle(customerLocation, radius);

            GeoRadiusCommandArgs args = GeoRadiusCommandArgs.newGeoRadiusArgs()
            .includeDistance()
            .sortAscending();

            GeoResults<GeoLocation<String>> results = redisTemplate.opsForGeo().radius("drivers:locations", searchArea, args);
            if(results != null){
                GeoResult<GeoLocation<String>> firstRes = results.getContent().get(0);
                GeoLocation<String> location = firstRes.getContent();
                String driverID = location.getName().toString();
                event.DriverID = driverID;
                event.Type = "START";    
            }
            
        }
        else if(event.Type == "COMPLETE"){

        }
        
    }
    public void addDriver(Driver driver){
        redisTemplate.opsForGeo().add(
            "drivers:locations", 
            // FIX: Ensure the first argument is Longitude, and the second is Latitude
            new Point(Double.parseDouble(driver.Lng), Double.parseDouble(driver.Lat)),
            driver.DriverID
        );
    }

}
