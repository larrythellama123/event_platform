package types

import (
	"github.com/segmentio/kafka-go"
)

type TripEvent struct {
	DriverID  string  `json:"driverId"`
	Type      string  `json:"type"` // REQUEST | MATCH | START | COMPLETE
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Timestamp string  `json:"timestamp"`
	Error     string  `json:"error"`
}

type Gateway struct {
	jobs            chan TripEvent
	reader          *kafka.Reader
	start_reader    *kafka.Reader
	complete_reader *kafka.Reader
	writer          *kafka.Writer
}

func NewGateway(reader_ *kafka.Reader, writer_ *kafka.Writer) *Gateway {
	g := &Gateway{}
	g.SetReader(reader_)
	g.SetWriter(writer_)
	return g
}

func (g *Gateway) SetReader(reader_ *kafka.Reader) {
	g.reader = reader_
}

func (g *Gateway) SetReaderSTART(reader_ *kafka.Reader) {
	g.start_reader = reader_
}
func (g *Gateway) SetReaderCOMPLETE(reader_ *kafka.Reader) {
	g.complete_reader = reader_
}

func (g *Gateway) SetWriter(writer_ *kafka.Writer) {
	g.writer = writer_
}

func (g *Gateway) GetWriter() *kafka.Writer {
	return g.writer
}

func (g *Gateway) GetReader() *kafka.Reader {
	return g.reader
}

func (g *Gateway) GetReaderSTART() *kafka.Reader {
	return g.start_reader
}

func (g *Gateway) GetReaderCOMPLETE() *kafka.Reader {
	return g.complete_reader
}

func (g *Gateway) GetJobs() chan TripEvent {
	return g.jobs
}
