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
							i := iterEvent.Value().Interface()
							interfaceEvent = flatten(i.(map[string]interface{}))
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

// flatten takes a map and returns a new one where nested maps are replaced
// by (maybe dot-delimited) keys.
func flatten(m map[string]interface{}) map[string]interface{} {
	o := make(map[string]interface{})
	for k, v := range m {
		switch child := v.(type) {
		case map[string]interface{}:
			nm := flatten(child)
			for /*nk*/ _, nv := range nm {
				nvs := fmt.Sprintf("%v", nv) //todo add child field name to the new key? like fields="validator_address 0x7b53b1ecab7da81205a27bf7fe1edae43a049dcd" or owner="AddressOwner 0x510b4f30c71f0d28061dc04937b8b8ef128c0571"
				//log.Printf("%v %v %v %v", k, nk, nv, nvs)
				//o[k+nk] = nvs
				//o[k+"."+nk] = nv
				//for _, nv := range nm {
				o[k] = nvs
			}
		default:
			o[k] = v
		}
	}
	return o
}

type EventSaver struct {
	name            string
	filenamePrefix  string
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
	err := json.Unmarshal(j, &e)
	if err != nil {
		//log.Fatalf("Unmarshal %v", err)
		//log.Printf("Unmarshal %v\n", err)
	}
	e.SetId(id)
	e.SetTimestamp(timestamp)
	//log.Printf("%v %v\n", t.name, e)

	err = t.parquetWriter.Write(e)
	if err != nil {
		log.Fatalf("Write %v", err)
	}
}

func (t *EventSaver) Start() {
	var err error

	// ParquetWriter

	t.parquetFilename = fmt.Sprintf("%s%s.parquet", t.name, t.filenamePrefix)
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

func NewEventSaver(name string, filenamePrefix string) *EventSaver {
	t := &EventSaver{name: name, filenamePrefix: filenamePrefix}

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

func startEventSavers(filenamePrefix string) {
	mapEventSaver = make(map[string]*EventSaver)

	for _, name := range []string{"transferObject", "publish", "coinBalanceChange", "moveEvent", "mutateObject", "deleteObject", "newObject"} {
		saver := NewEventSaver(name, filenamePrefix)
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

	var startTime, endTime time.Time
	var err error
	var filenameSuffix string

	dateStr := os.Getenv("SUI_ARCHIVE_DATE") //"2023-03-07"
	if dateStr != "" {
		startTime, err = time.Parse("2006-01-02", dateStr)
		if err != nil {
			log.Fatalf("startTime %v\n", err)
		}
		endTime = startTime.AddDate(0, 0, 1)

		filenameSuffix = fmt.Sprintf("-%s", dateStr)
	} else {
		startTimeStr := os.Getenv("SUI_ARCHIVE_START_TIME") //"2023-03-07T00:00:00Z"
		endTimeStr := os.Getenv("SUI_ARCHIVE_END_TIME")     //"2023-03-07T10:00:00Z"
		if startTimeStr == "" || endTimeStr == "" {
			log.Fatalln("specify with env variables either the date like SUI_ARCHIVE_DATE=2023-03-07 or both start and end times like SUI_ARCHIVE_START_TIME=2023-03-07T00:00:00Z SUI_ARCHIVE_END_TIME=2023-03-07T10:00:00Z")
		}

		startTime, err = time.Parse(time.RFC3339, startTimeStr)
		if err != nil {
			log.Fatalf("startTime %v\n", err)
		}
		endTime, err = time.Parse(time.RFC3339, endTimeStr)
		if err != nil {
			log.Fatalf("endTime %v\n", err)
		}

		filenameSuffix = fmt.Sprintf("-%s-%s", startTimeStr, endTimeStr)
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

	//allQuery := "All"
	//timeRange := &TimeRange{StartTime: /*startTime.Unix(), EndTime: endTime.Unix()*/ 1678169502291, EndTime: 1678169602291}
	timeRange := &TimeRange{StartTime: startTime.UnixMilli(), EndTime: endTime.UnixMilli()}
	timeRangeQuery := &TimeRangeQuery{TimeRange: *timeRange}

	method := "sui_getEvents"
	query := timeRangeQuery

	log.Printf("query %v with %v TimeRangeQuery %v %v %v %v\n", endpoint, method, startTime, endTime, timeRangeQuery, startCursor)

	startEventSavers(filenameSuffix)

	client := rpc.NewClient(endpoint)

	log.Printf("startCursor is %v\n", startCursor)

	response, err := client.Call(context.Background(), method, query, startCursor)
	if err != nil {
		log.Fatalf("giving up after failed first Call %v\n", err)
	}

	nextCursor := saveResponse(response)

	log.Printf("after startCursor nextCursor is %v\n", nextCursor)

	var failed, done bool

	for failed || !done {
		response, err = client.Call(context.Background(), method, query, nextCursor)

		if err != nil {
			failed = true
			log.Printf("retrying after failed Call %v\n", err)
		} else {
			failed = false

			nextCursor = saveResponse(response)

			if nextCursor == (EventID{}) {
				done = true
				log.Printf("done as received cursor %v\n", nextCursor)
			}
		}
	}

	stopEventSavers()
}
