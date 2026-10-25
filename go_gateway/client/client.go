package main

import (
	"log"
	"my-app/types"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

func handleBroadcast() {
	log.Println("loaded")

	u := url.URL{
		Scheme: "ws",
		Host:   "localhost:8080",
		Path:   "/events",
	}
	log.Println("About to dial")

	client, _, _ := websocket.DefaultDialer.Dial(u.String(), nil)

	log.Println("Dialed")

	defer client.Close()
	trip_event := types.TripEvent{
		DriverID:  "1",
		Type:      "REQUEST",
		Lat:       1.0,
		Lng:       1.0,
		Timestamp: "123",
		Error:     "nil",
	}
	if err := client.WriteJSON(trip_event); err != nil {
		log.Printf("Write error: %v", err)
		client.Close()
	}
	log.Println("Written")

	for {
		conn_read_err := client.ReadJSON(&trip_event)
		if conn_read_err != nil {
			break
		}
		if trip_event.Type == "START" {
			time.Sleep(2 * time.Second)
			if err := client.WriteJSON(trip_event); err != nil {
				log.Printf("Write error: %v", err)
				client.Close()
			}
		}
		if trip_event.Type == "COMPLETE" {
			//display on app maybe?
		}
	}
}

func main() {
	handleBroadcast()
}
