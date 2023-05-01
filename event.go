package main

import (
	"encoding/json"
	"github.com/jmoiron/sqlx"
	"log"
	"time"
	"strconv"
	"strings"
)

type EventRpc struct {
	Id EventID `json:"id"`
	Event
}

type EventDb struct {
	EventID
	Event
	TimestampDb time.Time
}

type Event struct {
	PackageId         string          `json:"packageId"`            // Move package where this event was emitted.
	TransactionModule string          `json:"transactionModule"`    // Move module where this event was emitted.
	Sender            string          `json:"sender"`               // Sender's Sui address.
	EventType         string          `json:"type"`                 // Move event type.
	ParsedJson        json.RawMessage `json:"parsedJson,omitempty"` // Parsed json value of the event
	Bcs               string          `json:"bcs,omitempty"`        // Base 58 encoded bcs bytes of the move event
	TimestampMs       string          `json:"timestampMs,omitempty"`
}

func NewEventDb(r EventRpc) (d EventDb) {
	d.TxDigest = r.Id.TxDigest
	d.EventSeq = r.Id.EventSeq

	d.PackageId = r.PackageId
	d.TransactionModule = r.TransactionModule
	d.Sender = r.Sender
	d.EventType = r.EventType
	d.Bcs = r.Bcs
	d.TimestampMs = r.TimestampMs

	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal(err)
	}

	timestampMs, err := strconv.ParseInt(r.TimestampMs, 10, 64)
	if err != nil {
		log.Fatal(err)
	}

	d.TimestampDb = time.UnixMilli(timestampMs).In(loc)

	bytes, err := json.Marshal(r.ParsedJson)
	if err != nil {
		log.Fatal(err)
	}

	s := string(bytes)

	//log.Printf("s %v", s)

	replaced := strings.Replace(s, `\u0000`, "", -1)

	//log.Printf("replaced %v", replaced)

	rawMessage := json.RawMessage(replaced)

	//log.Printf("rawMessage %v", rawMessage)

	d.ParsedJson = rawMessage

	return
}

type EventID struct {
	TxDigest string `json:"txDigest"`
	EventSeq string `json:"eventSeq"`
}

type EventTypeQuery struct {
	EventType string `json:"EventType"`
}

func NewEventTypeQuery(s string) *EventTypeQuery {
	return &EventTypeQuery{EventType: s}
}

type AllQuery struct {
	All []string `json:"All"`
}

func NewAllQuery() *AllQuery {
	q := &AllQuery{}
	q.All = make([]string, 0)
	return q
}

func queryMaxEventID(dataSourceName string) (maxEventID *EventID) {
	query := "select txDigest, eventSeq from event order by timestamp desc limit 1"

	db, err := sqlx.Open( /*"postgres"*/ "pgx", dataSourceName)
	if err != nil {
		log.Fatal(err)
	}

	eventID := EventID{}

	rows, err := db.Queryx(query)
	if err != nil {
		log.Fatal(err)
	}

	if rows.Next() {
		err = rows.StructScan(&eventID)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("queryMaxEventID %v", eventID)

		maxEventID = &eventID
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}

	return
}

type EventResponseResult struct {
	NextCursor  EventID    `json:"nextCursor"`
	Data        []EventRpc `json:"data"`
	HasNextPage bool       `json:"hasNextPage"`
}

func (t *EventResponseResult) Save(dataSourceName string) (countSaved int64) {
	if len(t.Data) == 0 {
		log.Println("no events found")
		return
	}

	eventsDb := make([]EventDb, 0)

	for _, r := range t.Data {
		d := NewEventDb(r)
		eventsDb = append(eventsDb, d)
	}

	db, err := sqlx.Open( /*"postgres"*/ "pgx", dataSourceName)
	if err != nil {
		log.Fatal(err)
	}

	insertQuery := "insert into Event (txDigest, eventSeq, timestamp, packageId, transactionModule, sender, type, parsedJson, bcs) values (:txdigest, :eventseq, :timestampdb, :packageid, :transactionmodule, :sender, :eventtype, :parsedjson, :bcs) on conflict on constraint Event_pkey do nothing"

	lastEvent := eventsDb[len(eventsDb)-1]

	result, err := db.NamedExec(insertQuery, eventsDb)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("inserted %v rows out of %v data with latest timestamp %v", rows, len(eventsDb), lastEvent.TimestampDb.UTC() /*, lastEvent*/)

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}

	//countSaved = len(eventsDb)
	countSaved = rows

	return
}
