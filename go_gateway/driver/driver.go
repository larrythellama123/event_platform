package driver
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
		Path:   "/driver_position",
	}
	log.Println("About to dial")

	client, _, _ := websocket.DefaultDialer.Dial(u.String(), nil)

	log.Println("Dialed")

	defer client.Close()
	driver_position := types.DriverPos{
		DriverID:  "1",
		Lat:       1.0,
		Lng:       1.0,
		Error:     "nil",
	}
	if err := client.WriteJSON(driver_position); err != nil {
		log.Printf("Write error: %v", err)
		client.Close()
	}
	log.Println("Written")


}

func main() {
	handleBroadcast()
}
