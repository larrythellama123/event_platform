package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

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
}

type Hub struct {
	mutex       sync.RWMutex
	connections map[string]*websocket.Conn
}

func newHub(rdb *redis.Client, channelName string) *Hub {
	return &Hub{
		connections: make(map[*websocket.Conn]bool),
	}
}

var validTypes = map[string]bool{
	"REQUEST": true, "MATCH": true, "START": true, "COMPLETE": true,
}

func (g *Hub) handleWS(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade failed: %v", err)
		return
	}
	mutex.Lock()
	g.connections[ws] = true
	mutex.Unlock()
	defer func() {
		mutex.Lock()
		delete(g.connections, ws)
		mutex.Unlock()
		ws.Close()
	}()

	for {
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var trip_event TripEvent
		json_err := json.NewDecoder(r.Body).Decode(&trip_event)
		if json_err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		validate_err := validate(trip_event)
		if validate_err != nil {
			http.Error(w, validate_err.Error(), http.StatusBadRequest)
			return
		}
		select {
		case g.jobs <- trip_event:
			w.WriteHeader(http.StatusAccepted)
		default:
			http.Error(w, "server busy, retry", http.StatusServiceUnavailable)
		}
	}

	// if r.Header.Get("Content-Type") != "application/json" {
	// 	http.Error(w, "Status Unsupported Media Type", http.StatusUnsupportedMediaType)
	// 	return
	// }

}

func handleBroadcast() {
	for {
		msg := TripEvent{}
		mutex.RLock()
		for client := range clients {
			if err := client.WriteJSON(msg); err != nil {
				log.Printf("Write error: %v", err)
				client.Close()
				mutex.Lock()
				delete(clients, client)
				mutex.Unlock()
			}
		}
		mutex.RUnlock()
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

type Gateway struct {
	jobs   chan TripEvent
	writer *kafka.Writer
}

func (g *Gateway) worker(id int) {
	for j := range g.jobs {
		payload, _ := json.Marshal(j)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := g.writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(j.TripID),
			Value: payload,
		})
		cancel()
		if err != nil {
			log.Printf("could not write message, worker id: %d", id)
		}
		log.Println("message was sent to kafka")
	}
}

func (g *Gateway) handle_request(w http.ResponseWriter, req *http.Request) {

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
		jobs:   make(chan TripEvent, 100),
		writer: writer,
	}
	for w := 0; w < 10; w++ {
		go g.worker(w)
	}

	fmt.Println("Server about to run")

	mux := http.NewServeMux()
	mux.HandleFunc("/events", g.handle_request)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: ":" + port, Handler: mux}
	go handleBroadcast()
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
