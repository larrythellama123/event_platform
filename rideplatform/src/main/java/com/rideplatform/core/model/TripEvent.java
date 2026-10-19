package com.rideplatform.core.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.validation.constraints.Null;

@Entity
@Table(name = "TripEvents")
public class TripEvent {
    @Null(groups = OnCreate.class, message = "tripID must be null or empty for new events.")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(nullable = false)
    public Long TripID; 

    @Column(nullable = false)
    public String DriverID;

    @Column(nullable = false)
    public String Type;

    @Column(nullable = false)
    public String Lat;

    @Column(nullable = false)
    public String Lng;

    @Column(nullable = false)
    public String TimeStamp;
}
