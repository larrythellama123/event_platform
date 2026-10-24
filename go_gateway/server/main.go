package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log"
	"my-app/types"
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

type LocalGateway struct {
	types.Gateway
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var browser_conn *websocket.Conn

func NewLocalGateway(reader *kafka.Reader, writer *kafka.Writer) *LocalGateway {
	g := &LocalGateway{}
	g.SetReader(reader)
	g.SetWriter(writer)
	return g
}

func (g *LocalGateway) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade failed: %v", err)
		return
	}
	var trip_event types.TripEvent
	for {
		conn_read_err := conn.ReadJSON(&trip_event)
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

		write_err := browser_conn.WriteJSON(trip_event)
		if write_err != nil {
			log.Printf("Broadcast failed to a client: %v", write_err)
		}
	}
}

func (g *LocalGateway) handleBrowserWS(w http.ResponseWriter, r *http.Request) {
	var err error
	browser_conn, err = upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade failed: %v", err)
		return
	}
}

func (g *LocalGateway) sendToKafka(payload []byte, type_ string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch type_ {
	case "REQUST":
		err := g.GetWriter().WriteMessages(ctx, kafka.Message{
			Topic: "requests",
			Value: payload,
		})
		if err != nil {
			log.Printf("could not write message")
			return
		}
	default:
		break
	}
	log.Println("message was sent to kafka")
}

func (g *LocalGateway) readFromKafkaSTART() {
	for {
		msg, err := g.GetReader().ReadMessage(context.Background())
		if err != nil {
			break
		}
	}
}

func (g *LocalGateway) readFromKafkaCOMPLETE() {
	for {
		msg, err := g.GetReader().ReadMessage(context.Background())
		if err != nil {
			break
		}
	}
}

var mutex sync.Mutex
var clients = make(map[string]*websocket.Conn)

var validTypes = map[string]bool{
	"REQUEST": true, "MATCH": true, "START": true, "COMPLETE": true,
}

func validate(e types.TripEvent) error {
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

var templates = template.Must(template.ParseFiles("/home/rohit/Desktop/event_platform/go_gateway/templates/base.html"))

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

	ctx_, cancel_ := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel_()

	err := writer.WriteMessages(ctx_, kafka.Message{
		Value: []byte("ping"),
	})

	if err != nil {
		log.Fatalf("Kafka writer check failed! Cluster might be down: %v", err)
	}
	log.Println("Kafka writer connectivity verified successfully.")

	g := NewLocalGateway(reader, writer)

	fmt.Println("Server about to run")

	mux := http.NewServeMux()
	mux.HandleFunc("/events", g.handleWS)
	mux.HandleFunc("/browser", g.handleBrowserWS)

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/base", handler)

	srv := &http.Server{Addr: ":" + port, Handler: mux}

	log.Println("Listening on :8080")
	http.ListenAndServe(":8080", mux)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop

	log.Println("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	close(g.GetJobs())
}
