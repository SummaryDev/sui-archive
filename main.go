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
	"os"
	"reflect"
	"strconv"
	"time"
)

func saveResponse(response *rpc.RPCResponse) EventID {
	if response.Error != nil {
		// rpc error handling goes here
		// check response.Error.Code, response.Error.Message and optional response.Error.Data
		log.Fatalf("rpc response error %v\n", response.Error)
	}

	var nextCursor EventID

	// loop thru properties of response
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
	//log.Printf("%v %v\n", t.name, e)

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
		log.Printf("cannot WriteStop %v %v\n", t.parquetFilename, err)
		return
	}
	log.Printf("finished writing %v\n", t.parquetFilename)

	err := t.parquetFile.Close()
	if err != nil {
		log.Printf("cannot Close %v %v\n", t.parquetFilename, err)
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
	case "moveEvent":
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

	for _, name := range []string{"transferObject", "publish", "coinBalanceChange", "moveEvent", "mutateObject", "deleteObject", "newObject"} {
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

type TimeRange struct {
	StartTime int64 `json:"startTime"`
	EndTime   int64 `json:"endTime"`
}
type TimeRangeQuery struct {
	TimeRange TimeRange `json:"TimeRange"`
}

func main() {

	startTimeStr := os.Getenv("SUI_ARCHIVE_START_TIME") //"2023-03-07T00:00:00Z"
	if startTimeStr == "" {
		log.Fatalln("specify time to start querying with env like SUI_ARCHIVE_START_TIME=2023-03-07T00:00:00Z")
	}
	endTimeStr := os.Getenv("SUI_ARCHIVE_END_TIME") //"2023-03-07T10:00:00Z"
	if endTimeStr == "" {
		log.Fatalln("specify time to end querying with env like SUI_ARCHIVE_END_TIME=2023-03-07T10:00:00Z")
	}

	var startCursor *EventID

	cursorTxDigest := os.Getenv("SUI_ARCHIVE_CURSOR_TXDIGEST")    //"Cmocd2cZ5iAJFWgShfvJPtoLy21DNPSiPWz5XKBpQUmH"
	cursorEventSeqStr := os.Getenv("SUI_ARCHIVE_CURSOR_EVENTSEQ") //"9"
	if cursorTxDigest != "" && cursorEventSeqStr != "" {
		cursorEventSeq, err := strconv.Atoi(cursorEventSeqStr)
		if err != nil {
			log.Fatalf("cannot parse SUI_ARCHIVE_CURSOR_EVENTSEQ %v\n", err)
		}

		startCursor = &EventID{TxDigest: cursorTxDigest, EventSeq: cursorEventSeq}
	}

	endpoint := os.Getenv("SUI_ARCHIVE_ENDPOINT") // "https://fullnode.devnet.sui.io"
	if endpoint == "" {
		endpoint = "https://fullnode.devnet.sui.io"
	}

	startTime, err := time.Parse(time.RFC3339, startTimeStr)
	if err != nil {
		log.Fatalf("startTime %v\n", err)
	}
	endTime, err := time.Parse(time.RFC3339, endTimeStr)
	if err != nil {
		log.Fatalf("endTime %v\n", err)
	}

	//allQuery := "All"
	//timeRange := &TimeRange{StartTime: /*startTime.Unix(), EndTime: endTime.Unix()*/ 1678169502291, EndTime: 1678169602291}
	//1678147200000 1678233600000
	//1678169402291 1678169502291
	timeRange := &TimeRange{StartTime: startTime.UnixMilli(), EndTime: endTime.UnixMilli()}
	timeRangeQuery := &TimeRangeQuery{TimeRange: *timeRange}

	method := "sui_getEvents"
	query := timeRangeQuery

	log.Printf("query %v with %v TimeRangeQuery %v %v %v startCursor is %v\n", endpoint, method, startTime, endTime, timeRangeQuery, startCursor)

	startEventSavers()

	client := rpc.NewClient(endpoint)

	log.Printf("startCursor is %v\n", startCursor)

	response, err := client.Call(context.Background(), method, query, startCursor)
	if err != nil {
		log.Fatalf("Call %v\n", err)
	}

	nextCursor := saveResponse(response)
	log.Printf("after startCursor nextCursor is %v\n", nextCursor)

	for nextCursor != (EventID{}) {
		response, err := client.Call(context.Background(), "sui_getEvents", query, nextCursor)
		if err != nil {
			log.Fatalf("Call %v\n", err)
		}

		nextCursor = saveResponse(response)
	}

	stopEventSavers()
}
