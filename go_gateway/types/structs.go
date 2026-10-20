package types

import (
	"context"
	"encoding/json"
	"log"
	"my-app/types"
	"net/http"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type TripEvent struct {
	TripID    string  `json:"tripId"`
	DriverID  string  `json:"driverId"`
	Type      string  `json:"type"` // REQUEST | MATCH | START | COMPLETE
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Timestamp string  `json:"timestamp"`
	Error     string  `json:"error"`
}

type Hub struct {
	mutex sync.RWMutex
}

type Gateway struct {
	jobs   chan TripEvent
	reader *kafka.Reader
	writer *kafka.Writer
}

func (g *Gateway) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade failed: %v", err)
		return
	}
	var trip_event types.TripEvent
	for {
		conn_read_err := conn.ReadJSON(trip_event)
		if conn_read_err != nil {
			break
		}
		validate_err := validate(trip_event)
		if validate_err != nil {
			trip_event.Error = "VALIDATION_ERROR"
			conn.WriteJSON(trip_event)
			continue
		}
		payload, _ := json.Marshal(trip_event)
		g.sendToKafka(payload)
	}
}

func (g *Gateway) sendToKafka(payload []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := g.writer.WriteMessages(ctx, kafka.Message{
		Value: payload,
	})
	cancel()
	if err != nil {
		log.Printf("could not write message")
	}
	log.Println("message was sent to kafka")
}
