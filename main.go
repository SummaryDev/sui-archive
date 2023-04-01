package main

import (
	"context"
	"fmt"
	//_ "github.com/lib/pq"
	_ "github.com/jackc/pgx/stdlib"
	rpc "github.com/ybbus/jsonrpc/v3"
	"log"
	"os"
	"strconv"
	"time"
)

func saveResponse(response *rpc.RPCResponse, dataSourceName string) (eventResponseResult *EventResponseResult, countSaved int64) {
	if response.Error != nil {
		// rpc error handling goes here
		// check response.Error.Code, response.Error.Message and optional response.Error.Data
		log.Fatalf("rpc response error %v\n", response.Error)
	}

	err := response.GetObject(&eventResponseResult)
	if err != nil {
		log.Fatalf("cannot GetObject for EventResponseResult %v\n", err)
	}

	countSaved = eventResponseResult.Save(dataSourceName)

	return
}

func getArgs() (endpoint string, timeRangeQuery *TimeRangeQuery, startCursor *EventID, cronSeconds int, dataSourceName string, eventTypeQuery *EventTypeQuery) {
	var err error

	var startTime, endTime time.Time

	dateStr := os.Getenv("SUI_ARCHIVE_DATE")            //"2023-03-07"
	startTimeStr := os.Getenv("SUI_ARCHIVE_START_TIME") //"2023-03-07T00:00:00Z"
	endTimeStr := os.Getenv("SUI_ARCHIVE_END_TIME")     //"2023-03-07T10:00:00Z"
	cronStr := os.Getenv("SUI_ARCHIVE_CRON_SECONDS")    //60

	if dateStr != "" {
		startTime = parseTimeFromDateStr(dateStr)
		endTime = startTime.AddDate(0, 0, 1)
		timeRangeQuery = NewTimeRangeQuery(startTime, endTime)
	}

	if startTimeStr != "" && endTimeStr != "" {
		startTime = parseTimeFromTimeStr(startTimeStr)
		endTime = parseTimeFromTimeStr(endTimeStr)
		timeRangeQuery = NewTimeRangeQuery(startTime, endTime)
	}

	if cronStr != "" {
		cronSeconds, err = strconv.Atoi(cronStr)
		if err != nil {
			log.Fatalf("cronStr %v\n", err)
		}
	}

	eventType := os.Getenv("SUI_ARCHIVE_EVENT_TYPE")
	if eventType != "" {
		eventTypeQuery = NewEventTypeQuery(eventType)
	}

	if cronStr == "" && (startTimeStr == "" && endTimeStr == "") && dateStr == "" && eventType == "" {
		log.Fatalln("specify with env variables either the date like SUI_ARCHIVE_DATE=2023-03-07 or both start and end times like SUI_ARCHIVE_START_TIME=2023-03-07T00:00:00Z SUI_ARCHIVE_END_TIME=2023-03-07T10:00:00Z or cron frequency in seconds and start time like SUI_ARCHIVE_CRON_SECONDS=60 SUI_ARCHIVE_START_TIME=2023-03-07T00:00:00Z or specific event to query like SUI_ARCHIVE_EVENT_TYPE=MoveEvent")
	}

	cursorTxDigest := os.Getenv("SUI_ARCHIVE_CURSOR_TXDIGEST")    //"Cmocd2cZ5iAJFWgShfvJPtoLy21DNPSiPWz5XKBpQUmH"
	cursorEventSeqStr := os.Getenv("SUI_ARCHIVE_CURSOR_EVENTSEQ") //"9"
	if cursorTxDigest != "" && cursorEventSeqStr != "" {
		cursorEventSeq, err := strconv.Atoi(cursorEventSeqStr)
		if err != nil {
			log.Fatalf("cannot parse SUI_ARCHIVE_CURSOR_EVENTSEQ %v\n", err)
		}

		startCursor = &EventID{TxDigest: cursorTxDigest, EventSeq: cursorEventSeq}
	}

	endpoint = os.Getenv("SUI_ARCHIVE_ENDPOINT") // https://fullnode.devnet.sui.io https://explorer-rpc.devnet.sui.io
	if endpoint == "" {
		endpoint = "https://fullnode.devnet.sui.io"
	}

	schema := os.Getenv("SUI_ARCHIVE_SCHEMA")
	if schema == "" {
		schema = "sui_devnet"
	}

	dataSourceName = fmt.Sprintf("host=%v dbname=%v user=%v password=%v search_path=%v", os.Getenv("PGHOST"), os.Getenv("PGDATABASE"), os.Getenv("PGUSER"), os.Getenv("PGPASSWORD"), schema)

	return
}

func query(endpoint string, dataSourceName string, query interface{}, startCursor *EventID) (nomore bool) {
	method := "suix_queryEvents"

	log.Printf("query %v with %v %v %v\n", endpoint, method, query, startCursor)

	client := rpc.NewClient(endpoint)

	var failed, done bool

	nextCursor := startCursor

	for failed || !done {
		response, err := client.Call(context.Background(), method, query, nextCursor)

		if err != nil {
			failed = true

			switch e := err.(type) {
			case *rpc.HTTPError:
				if e.Code == 429 {
					log.Printf("sleeping for 10s then retrying after Call failed with too many requests HTTPError=%v\n", err)
					time.Sleep(time.Second * 10)
				} else if e.Code == 503 || e.Code == 504 {
					log.Printf("sleeping for 5s then retrying after Call failed with server overloaded HTTPError=%v\n", err)
					time.Sleep(time.Second * 5)
				} else {
					log.Printf("retrying immediately after Call failed with HTTPError=%v\n", err)
				}
			default:
				log.Printf("retrying immediately after Call failed with err=%v\n", err)
			}

		} else if response == nil {
			failed = true
			log.Printf("retrying after Call failed with response=%v\n", response)
		} else if response.Error != nil {
			if response.Error.Code == -32602 {
				done = true
				nomore = true
				log.Printf("done as received indication there are no more events: %v\n", response.Error)
				//} else if response.Error.Code == -32000 {
				//	done = true
				//	nomore = true
				//	log.Printf("done as received an error we cannot recover from: %v\n", response.Error)
			} else {
				failed = true
				log.Printf("retrying after Call failed with %v\n", response.Error)
			}

		} else {
			failed = false

			eventResponseResult, countSaved := saveResponse(response, dataSourceName)

			nextCursor = &eventResponseResult.NextCursor

			hasNextPage := eventResponseResult.HasNextPage

			if *nextCursor == (EventID{}) {
				done = true
				nomore = true
				log.Println("done as received empty cursor")
			}

			if !hasNextPage {
				done = true
				log.Println("done as received false hasNextPage")
			}

			if countSaved == 0 {
				done = true
				log.Println("done as saved no events")
			}
		}
	}

	return nomore
}

func main() {
	endpoint, timeRangeQuery, startCursor, cronSeconds, dataSourceName, eventTypeQuery := getArgs()

	if cronSeconds > 0 {
		window := time.Duration(cronSeconds) * time.Second
		sleep := time.Duration(10) * time.Second

		for {
			unsavedEventsQuery, final := unsavedEventsQuery(dataSourceName, timeRangeQuery, window)
			log.Printf("repeating unsavedEventsQuery for events in a %v window with %v", window, unsavedEventsQuery)

			nomore := query(endpoint, dataSourceName, unsavedEventsQuery, nil)

			if final {
				log.Printf("quitting as unsavedEventsQuery window end moved beyond the range specified by input %v", timeRangeQuery)
				break
			}

			if nomore {
				log.Printf("likely there are no more recent events, sleeping for %v", sleep)
				time.Sleep(sleep)
			}
		} // todo replace infinite loop with another mechanism perhaps reacting to program termination signals

	} else if eventTypeQuery != nil {
		query(endpoint, dataSourceName, eventTypeQuery, startCursor)
	}
}
