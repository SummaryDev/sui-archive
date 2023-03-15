package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jmoiron/sqlx"

	//_ "github.com/lib/pq"
	_ "github.com/jackc/pgx/stdlib"
	rpc "github.com/ybbus/jsonrpc/v3"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
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
					//log.Printf("cannot handle event %v\n", keyEvent)
				} else {
					saver.Save(interfaceEvent, id, timestamp)
				}
			}
		case "nextCursor":
			nextCursor = NewEventID(interfaceResponse)
			log.Printf("nextCursor %v\n", nextCursor)
		}
	}

	commitEventSavers()

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

var eventNames = []string{"publish", "transferObject", "coinBalanceChange", "moveEvent", "mutateObject", "deleteObject", "newObject"}

var mapEventSaver = map[string]EventSaver{}

func startFileEventSavers(filenameSuffix string, folder string) {
	for _, name := range eventNames {
		saver := NewFileEventSaver(name, filenameSuffix, folder)
		saver.Start()
		mapEventSaver[name] = saver
	}
}

func startDatabaseEventSavers(dataSourceName string) {
	for _, name := range eventNames {
		saver := NewDatabaseEventSaver(name, dataSourceName)
		saver.Start()
		mapEventSaver[name] = saver
	}
}

func queryMaxTimestamp(dataSourceName string) time.Time {
	subqueries := make([]string, len(eventNames))

	i := 0
	for _, saver := range mapEventSaver {
		subqueries[i] = fmt.Sprintf("select max(timestamp) m from %s", saver.EventType().Name()) /*PublishEvent*/
		i++
	}

	db, err := sqlx.Open( /*"postgres"*/ "pgx", dataSourceName)
	if err != nil {
		log.Fatal(err)
	}

	//maxTimestamp = d.QueryTimestamp("select cast(extract(epoch from max(m))::dec*1000 as bigint) from (" + strings.Join(subqueries, " union all ") + ") sub")

	query := fmt.Sprintf("select max(m) from (%s) sub", strings.Join(subqueries, " union all "))

	maxTimestamp := time.Now().UTC()
	row := db.QueryRow(query)
	err = row.Scan(&maxTimestamp)
	if err != nil {
		log.Fatal(err)
	}

	return maxTimestamp
}

func commitEventSavers() {
	for _, saver := range mapEventSaver {
		saver.Commit()
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

func (t *TimeRangeQuery) String() string {
	return fmt.Sprintf("StartTime %v %v EndTime %v %v", t.TimeRange.StartTime, time.UnixMilli(t.TimeRange.StartTime).UTC(), t.TimeRange.EndTime, time.UnixMilli(t.TimeRange.EndTime).UTC())
}

func NewTimeRangeQuery(startTime time.Time, endTime time.Time) *TimeRangeQuery {
	return &TimeRangeQuery{TimeRange: TimeRange{StartTime: startTime.UnixMilli(), EndTime: endTime.UnixMilli()}}
}

func getArgs() (endpoint string, timeRangeQuery *TimeRangeQuery, filenameSuffix string, startCursor *EventID, cronSeconds int, dataSourceName string, folder string) {
	var err error

	var startTime, endTime time.Time

	dateStr := os.Getenv("SUI_ARCHIVE_DATE")            //"2023-03-07"
	startTimeStr := os.Getenv("SUI_ARCHIVE_START_TIME") //"2023-03-07T00:00:00Z"
	endTimeStr := os.Getenv("SUI_ARCHIVE_END_TIME")     //"2023-03-07T10:00:00Z"
	cronStr := os.Getenv("SUI_ARCHIVE_CRON_SECONDS")    //60

	if dateStr != "" {
		startTime, err = time.Parse("2006-01-02", dateStr)
		if err != nil {
			log.Fatalf("startTime %v\n", err)
		}
		endTime = startTime.AddDate(0, 0, 1)

		filenameSuffix = fmt.Sprintf("-%s", dateStr)
	} else if startTimeStr != "" && endTimeStr != "" {
		startTime, err = time.Parse(time.RFC3339, startTimeStr)
		if err != nil {
			log.Fatalf("startTime %v\n", err)
		}
		endTime, err = time.Parse(time.RFC3339, endTimeStr)
		if err != nil {
			log.Fatalf("endTime %v\n", err)
		}

		filenameSuffix = fmt.Sprintf("-%s-%s", startTimeStr, endTimeStr)
	} else if cronStr != "" {
		cronSeconds, err = strconv.Atoi(cronStr)
		if err != nil {
			log.Fatalf("cronStr %v\n", err)
		}
	} else {
		log.Fatalln("specify with env variables either the date like SUI_ARCHIVE_DATE=2023-03-07 or both start and end times like SUI_ARCHIVE_START_TIME=2023-03-07T00:00:00Z SUI_ARCHIVE_END_TIME=2023-03-07T10:00:00Z or cron frequency in seconds and start time like SUI_ARCHIVE_CRON_SECONDS=60 SUI_ARCHIVE_START_TIME=2023-03-07T00:00:00Z")
	}

	//allQuery := "All"
	//timeRange := &TimeRange{StartTime: /*startTime.Unix(), EndTime: endTime.Unix()*/ 1678169502291, EndTime: 1678169602291}
	timeRangeQuery = NewTimeRangeQuery(startTime, endTime)

	cursorTxDigest := os.Getenv("SUI_ARCHIVE_CURSOR_TXDIGEST")    //"Cmocd2cZ5iAJFWgShfvJPtoLy21DNPSiPWz5XKBpQUmH"
	cursorEventSeqStr := os.Getenv("SUI_ARCHIVE_CURSOR_EVENTSEQ") //"9"
	if cursorTxDigest != "" && cursorEventSeqStr != "" {
		cursorEventSeq, err := strconv.Atoi(cursorEventSeqStr)
		if err != nil {
			log.Fatalf("cannot parse SUI_ARCHIVE_CURSOR_EVENTSEQ %v\n", err)
		}

		startCursor = &EventID{TxDigest: cursorTxDigest, EventSeq: cursorEventSeq}
	}

	endpoint = os.Getenv("SUI_ARCHIVE_ENDPOINT") // "https://fullnode.devnet.sui.io"
	if endpoint == "" {
		endpoint = "https://fullnode.devnet.sui.io"
	}

	schema := os.Getenv("SUI_ARCHIVE_SCHEMA")
	if schema == "" {
		schema = "sui_devnet"
	}

	dataSourceName = fmt.Sprintf("host=%v dbname=%v user=%v password=%v search_path=%v", os.Getenv("PGHOST"), os.Getenv("PGDATABASE"), os.Getenv("PGUSER"), os.Getenv("PGPASSWORD"), schema)

	folder = os.Getenv("SUI_ARCHIVE_FOLDER") // "./sui-archive-data"
	if folder == "" {
		folder = "."
	}

	return endpoint, timeRangeQuery, filenameSuffix, startCursor, cronSeconds, dataSourceName, folder
}

func queryTimeRange(endpoint string, timeRangeQuery *TimeRangeQuery, startCursor *EventID) (nomore bool) {
	method := "sui_getEvents"

	log.Printf("query %v with %v TimeRangeQuery %v %v\n", endpoint, method, timeRangeQuery, startCursor)

	client := rpc.NewClient(endpoint)

	log.Printf("startCursor is %v\n", startCursor)

	response, err := client.Call(context.Background(), method, timeRangeQuery, startCursor)
	if err != nil {
		log.Fatalf("giving up after failed first Call %v\n", err)
	}

	nextCursor := saveResponse(response)

	log.Printf("after startCursor %v nextCursor is %v\n", startCursor, nextCursor)

	var failed, done bool

	for failed || !done {
		response, err = client.Call(context.Background(), method, timeRangeQuery, nextCursor)

		if err != nil {
			failed = true
			log.Printf("retrying after Call failed with err=%v\n", err)
		} else if response == nil {
			failed = true
			log.Printf("retrying after Call failed with response=%v\n", response)
		} else if response.Error != nil {
			if response.Error.Code == -32602 {
				done = true
				nomore = true
				log.Printf("done as received indication there are no more events with response.Error=%v\n", response.Error)
			} else {
				failed = true
				log.Printf("retrying after Call failed with response.Error=%v %v\n", response.Error)
			}

		} else {
			failed = false

			nextCursor = saveResponse(response)

			if nextCursor == (EventID{}) {
				done = true
				log.Printf("done as received cursor %v\n", nextCursor)
			}
		}
	}

	return nomore
}

func main() {
	endpoint, timeRangeQuery, filenameSuffix, startCursor, cronSeconds, dataSourceName, folder := getArgs()

	if cronSeconds > 0 {
		window := time.Duration(cronSeconds) * time.Second //10
		sleep := time.Duration(cronSeconds/2) * time.Second

		startDatabaseEventSavers(dataSourceName)

		//scheduler := gocron.NewScheduler(time.UTC)
		//
		//_, err := scheduler.Every(cronSeconds).Seconds().Do(func() {

		for {
			log.Printf("repeating query for events in a %v window", window)

			maxTimestamp := queryMaxTimestamp(dataSourceName)
			nextTimestamp := maxTimestamp.Add(time.Millisecond)

			//log.Printf("latest of all events is maxTimestamp=%v %v, starting query with nextTimestamp=%v %v", maxTimestamp.UnixMilli(), maxTimestamp.UTC(), nextTimestamp.UnixMilli(), nextTimestamp.UTC())

			startTime := nextTimestamp
			//endTime := time.Now().AddDate(0, 0, -5) /*maxTimestamp.UnixMilli() + 60_000*60*24*/
			//endTime := startTime.AddDate(0, 0, 1)
			endTime := startTime.Add(window)

			unsavedEventsQuery := NewTimeRangeQuery(startTime, endTime)
			nomore := queryTimeRange(endpoint, unsavedEventsQuery, nil)

			if nomore {
				log.Printf("likely there are no more recent events, sleeping for %v", sleep)
				time.Sleep(sleep)
			}
		}

		//})
		//if err != nil {
		//	log.Fatalf("cannot schedule job %v", err)
		//}
		//log.Printf("scheduled query to run every %v seconds", cronSeconds)
		//
		//scheduler.StartBlocking()

		stopEventSavers()

	} else {
		startFileEventSavers(filenameSuffix, folder)
		queryTimeRange(endpoint, timeRangeQuery, startCursor)
		stopEventSavers()
	}
}
