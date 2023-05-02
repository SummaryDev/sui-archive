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
	} else {
		cronSeconds = 60 * 60 * 24 // 86400
	}

	eventType := os.Getenv("SUI_ARCHIVE_EVENT_TYPE")
	if eventType != "" {
		eventTypeQuery = NewEventTypeQuery(eventType)
	}

	//if cronStr == "" && (startTimeStr == "" && endTimeStr == "") && dateStr == "" && eventType == "" {
	//	log.Fatalln("specify with env variables either the date like SUI_ARCHIVE_DATE=2023-03-07 or both start and end times like SUI_ARCHIVE_START_TIME=2023-03-07T00:00:00Z SUI_ARCHIVE_END_TIME=2023-03-07T10:00:00Z or cron frequency in seconds and start time like SUI_ARCHIVE_CRON_SECONDS=60 SUI_ARCHIVE_START_TIME=2023-03-07T00:00:00Z or specific event to query like SUI_ARCHIVE_EVENT_TYPE=MoveEvent")
	//}

	cursorTxDigest := os.Getenv("SUI_ARCHIVE_CURSOR_TXDIGEST") //"Cmocd2cZ5iAJFWgShfvJPtoLy21DNPSiPWz5XKBpQUmH"
	cursorEventSeq := os.Getenv("SUI_ARCHIVE_CURSOR_EVENTSEQ") //"9"

	log.Printf("cursorTxDigest %v cursorEventSeq %v", cursorTxDigest, cursorEventSeq)

	if cursorTxDigest != "" && cursorEventSeq != "" {
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

	client := rpc.NewClient(endpoint)

	var failed, done bool

	nextCursor := startCursor

	for failed || !done {
		log.Printf("query %v with %v %v %v\n", endpoint, method, query, nextCursor)

		response, err := client.Call(context.Background(), method, query, nextCursor, 100)

		//log.Printf("response %v", response)

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
			log.Printf("retrying immediately after Call failed with response=%v\n", response)
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
				log.Printf("retrying immediately after Call failed with %v\n", response.Error)
			}

		} else {
			failed = false

			var eventResponseResult *EventResponseResult

			err := response.GetObject(&eventResponseResult)
			if err != nil {
				log.Fatalf("cannot GetObject for EventResponseResult %v\n", err)
			}

			countSaved := eventResponseResult.Save(dataSourceName)

			nextCursor = &eventResponseResult.NextCursor

			hasNextPage := eventResponseResult.HasNextPage

			if *nextCursor == (EventID{}) {
				done = true
				nomore = true // todo find better indication there are no more results
				log.Println("done as received empty cursor")
			}

			if !hasNextPage {
				done = true
				nomore = true
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

	sleep := time.Duration(10) * time.Second
	var q interface{}

	if timeRangeQuery != nil {
		window := time.Duration(cronSeconds) * time.Second
		q, final := unsavedEventsTimeRangeQuery(dataSourceName, timeRangeQuery, window)
		log.Printf("repeating query for events in a %v window with %v", window, q)

		for {
			nomore := query(endpoint, dataSourceName, q, nil)

			if final {
				log.Printf("quitting as query window end moved beyond the range specified by input %v", timeRangeQuery)
				break
			}

			if nomore {
				log.Printf("likely there are no more recent events, sleeping for %v", sleep)
				time.Sleep(sleep)
			}
		}
	} else if eventTypeQuery != nil {
		query(endpoint, dataSourceName, eventTypeQuery, startCursor)
	} else {
		var cursor *EventID

		if startCursor == nil {
			cursor = queryMaxEventID(dataSourceName)
		} else {
			cursor = startCursor
		}

		q = NewAllQuery()

		for {
			log.Printf("repeating query for all events with cursor %v", cursor)

			nomore := query(endpoint, dataSourceName, q, cursor)

			if nomore {
				log.Printf("no more recent events, sleeping for %v", sleep)
				time.Sleep(sleep)

				cursor = queryMaxEventID(dataSourceName)
			}
		}
	}
}
