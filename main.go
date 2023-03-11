package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	rpc "github.com/ybbus/jsonrpc/v3"
	"log"
	"reflect"
)

func saveResponse(response *rpc.RPCResponse) EventID {
	if response.Error != nil {
		// rpc error handling goes here
		// check response.Error.Code, response.Error.Message and optional response.Error.Data
		log.Fatalf("rpc response error %v\n", response.Error)
	}

	var nextCursor EventID

	iterResponse := reflect.ValueOf(response.Result).MapRange()
	for iterResponse.Next() {
		keyResponse := iterResponse.Key().String()
		interfaceResponse := iterResponse.Value().Interface()

		switch keyResponse {
		case "data":
			arrayData := iterResponse.Value().Interface().([]interface{})

			// loop thru elements of data array
			for _, datum := range arrayData {
				var id EventID
				var timestamp int64
				var keyEvent string
				var interfaceEvent interface{}

				// loop thru properties of the array: id, timestamp, event
				iterDatum := reflect.ValueOf(datum).MapRange()
				for iterDatum.Next() {
					keyDatum := iterDatum.Key().String()
					interfaceDatum := iterDatum.Value().Interface()

					switch keyDatum {
					case "timestamp":
						timestamp, _ = iterDatum.Value().Interface().(json.Number).Int64()
					case "id":
						id = NewEventID(interfaceDatum)
					case "event":
						// the only child of the event property is a specific event: publish, newObject etc.
						iterEvent := reflect.ValueOf(interfaceDatum).MapRange()
						for iterEvent.Next() {
							keyEvent = iterEvent.Key().String()
							interfaceEvent = iterEvent.Value().Interface()
						}
					}
				}

				saver := mapEventSaver[keyEvent]
				if saver == nil {
					log.Printf("cannot handle event %v\n", keyEvent)
				} else {
					saver.Save(interfaceEvent, id, timestamp)
				}
			}
		case "nextCursor":
			nextCursor = NewEventID(interfaceResponse)
			log.Printf("nextCursor %v\n", nextCursor)
		}
	}

	return nextCursor
}

type EventSaver struct {
	name            string
	parquetWriter   *writer.ParquetWriter
	parquetFile     source.ParquetFile
	parquetFilename string
	eventType       reflect.Type
}

func (t *EventSaver) Save(interfaceEvent interface{}, id EventID, timestamp int64) {
	j, _ := json.Marshal(interfaceEvent)

	i := reflect.New(t.eventType).Interface()
	var e Event
	e = i.(Event)
	json.Unmarshal(j, &e)
	e.SetId(id)
	e.SetTimestamp(timestamp)
	log.Printf("%v %v\n", t.name, e)

	err := t.parquetWriter.Write(e)
	if err != nil {
		log.Fatalf("Write error %v", err)
	}
}

func (t *EventSaver) Start() {
	var err error

	// ParquetWriter

	t.parquetFilename = fmt.Sprintf("%s.parquet", t.name)
	t.parquetFile, err = local.NewLocalFileWriter(t.parquetFilename)
	if err != nil {
		log.Fatal("Can't create local file", err)
	}

	e := reflect.New(t.eventType).Interface()
	t.parquetWriter, err = writer.NewParquetWriter(t.parquetFile, e, 4)

	if err != nil {
		log.Fatal("Can't create parquet writer", err)
	}

	t.parquetWriter.RowGroupSize = 128 * 1024 * 1024 //128M
	t.parquetWriter.PageSize = 8 * 1024              //8K
	t.parquetWriter.CompressionType = parquet.CompressionCodec_SNAPPY
}

func (t *EventSaver) Stop() {
	// ParquetWriter

	if err := t.parquetWriter.WriteStop(); err != nil {
		log.Printf("cannot WriteStop %v %v", t.parquetFilename, err)
		return
	}
	log.Printf("finished writing %v\n", t.parquetFilename)

	err := t.parquetFile.Close()
	if err != nil {
		log.Printf("cannot Clos %v %v", t.parquetFilename, err)
		return
	}
}

func NewEventSaver(name string) *EventSaver {
	t := &EventSaver{name: name}

	switch t.name {
	case "transferObject":
		t.eventType = reflect.TypeOf(TransferObjectEvent{})
	case "publish":
		t.eventType = reflect.TypeOf(PublishEvent{})
	case "coinBalanceChange":
		t.eventType = reflect.TypeOf(CoinBalanceChangeEvent{})
	case "move":
		t.eventType = reflect.TypeOf(MoveEvent{})
	case "mutateObject":
		t.eventType = reflect.TypeOf(MutateObjectEvent{})
	case "deleteObject":
		t.eventType = reflect.TypeOf(DeleteObjectEvent{})
	case "newObject":
		t.eventType = reflect.TypeOf(NewObjectEvent{})
	}

	return t
}

var mapEventSaver map[string]*EventSaver

func startEventSavers() {
	mapEventSaver = make(map[string]*EventSaver)

	for _, name := range []string{"transferObject", "publish", "coinBalanceChange", "move", "mutateObject", "deleteObject", "newObject"} {
		saver := NewEventSaver(name)
		saver.Start()
		mapEventSaver[name] = saver
	}
}

func stopEventSavers() {
	for _, saver := range mapEventSaver {
		saver.Stop()
	}
}

func main() {
	log.Println("sui-archive")

	startEventSavers()

	client := rpc.NewClient("https://fullnode.devnet.sui.io")

	query := "All"

	response, err := client.Call(context.Background(), "sui_getEvents", query, nil)
	if err != nil {
		log.Fatalf("Call %v\n", err)
	}

	cursor := saveResponse(response)
	log.Printf("first cursor %v\n", cursor)

	//for cursor != (EventID{}) {
	//	response, err := client.Call(context.Background(), "sui_getEvents", query, cursor)
	//	if err != nil {
	//		log.Fatalf("Call %v\n", err)
	//	}
	//
	//	cursor = saveResponse(response)
	//}

	stopEventSavers()
}
