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
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type TripEvent struct {
	EventID   string  `json:"eventId"`
	TripID    string  `json:"tripId"`
	DriverID  string  `json:"driverId"`
	Type      string  `json:"type"` // REQUEST | MATCH | START | COMPLETE
	Zone      string  `json:"zone"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Timestamp string  `json:"timestamp"`
}

var validTypes = map[string]bool{
	"REQUEST": true, "MATCH": true, "START": true, "COMPLETE": true,
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
	if req.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "Status Unsupported Media Type", http.StatusUnsupportedMediaType)
		return
	}

	var trip_event TripEvent
	err := json.NewDecoder(req.Body).Decode(&trip_event)
	if err != nil {
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

func main() {
	port := "8080"
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "requests",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	g := &Gateway{}
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
