package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

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

var mutex sync.Mutex
var clients = make(map[string]*websocket.Conn)

var validTypes = map[string]bool{
	"REQUEST": true, "MATCH": true, "START": true, "COMPLETE": true,
}

func (g *Gateway) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade failed: %v", err)
		return
	}
	var trip_event TripEvent
	for {
		conn_read_err := conn.ReadJSON(trip_event)
		if conn_read_err != nil {
			break
		}
		validate_err := validate(trip_event)
		if validate_err != nil {
			trip_event.Error = "VALIDATION_ERROR"
			conn.writeJSON(trip_event)
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

func handleBroadcast() {
	u := url.URL{
		Scheme: "wss",
		Host:   "localhost:8080",
		Path:   "/events",
	}
	client, _, _ := websocket.DefaultDialer.Dial(u.String(), nil)
	defer client.Close()
	trip_event := TripEvent{}
	if err := client.WriteJSON(trip_event); err != nil {
		log.Printf("Write error: %v", err)
		client.Close()
	}

	for {
		conn_read_err := client.ReadJSON(trip_event)
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
		if trip_event.Type == "Complete" {
			//display on app maybe?
		}
	}
}

func validate(e TripEvent) error {
	if strings.TrimSpace(e.TripID) == "" {
		return errors.New("tripId is required")
	}
	if strings.TrimSpace(e.DriverID) == "" {
		return errors.New("driverId is required")
	}
	if !validTypes[e.Type] {
		return errors.New("type must be one of REQUEST, MATCH, START, COMPLETE")
	}
	if e.Lat < -90 || e.Lat > 90 || e.Lng < -180 || e.Lng > 180 {
		return errors.New("lat/lng out of range")
	}
	return nil
}

func main() {
	port := "8080"

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "start_events",
		GroupID: "consumer-group;",
	})
	defer reader.Close()

	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "requests",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	g := &Gateway{
		reader: reader,
		writer: writer,
	}

	fmt.Println("Server about to run")

	mux := http.NewServeMux()
	mux.HandleFunc("/events", g.handleWS)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: ":" + port, Handler: mux}
	for i := 0; i < 5; i++ {
		go handleBroadcast()
	}
	http.ListenAndServe(":8080", nil)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop

	log.Println("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	close(g.jobs)
}
