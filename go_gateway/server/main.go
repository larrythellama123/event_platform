package main

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"
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

var mutex sync.Mutex
var clients = make(map[string]*websocket.Conn)

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

var templates = template.Must(template.ParseFiles("./templates/base.html"))

func handler(w http.ResponseWriter, r *http.Request) {

	b := struct {
		Title        template.HTML
		BusinessName string
		Slogan       string
	}{
		Title:        template.HTML("Business &verbar; Landing"),
		BusinessName: "Business,",
		Slogan:       "we get things done.",
	}
	err := templates.ExecuteTemplate(w, "base", &b)
	if err != nil {
		http.Error(w, fmt.Sprintf("index: couldn't parse template: %v", err), http.StatusInternalServerError)
		return
	}

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
	mux.HandleFunc("/base", handler)

	srv := &http.Server{Addr: ":" + port, Handler: mux}
	for i := 0; i < 1; i++ {
		go handleBroadcast()
	}

	log.Println("Listening on :8080")
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
