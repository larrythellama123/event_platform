package com.rideplatform.core.model;

import java.math.BigDecimal;

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
    public Integer TripID; 

    @Column(nullable = false)
    public String driverId;

    @Column(nullable = false)
    public String type;

    @Column(nullable = false)
    public Double lat;

    @Column(nullable = false)
    public Double lng;

    @Column(nullable = false)
    public String timestamp;
    
    @Column(nullable = false)
    public String error;
}
