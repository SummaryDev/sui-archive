package main

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"log"
	"time"
	"strconv"
)

type TimeRange struct {
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
}

type TimeRangeQuery struct {
	TimeRange TimeRange `json:"TimeRange"`
}

func (t *TimeRangeQuery) String() string {
	//return fmt.Sprintf("StartTime %v %v EndTime %v %v", t.TimeRange.StartTime, time.UnixMilli(t.TimeRange.StartTime).UTC(), t.TimeRange.EndTime, time.UnixMilli(t.TimeRange.EndTime).UTC())
	startTime, _ := strconv.ParseInt(t.TimeRange.StartTime, 10, 64)
	endTime, _ := strconv.ParseInt(t.TimeRange.EndTime, 10, 64)
	return fmt.Sprintf("%v %v", time.UnixMilli(startTime).UTC(), time.UnixMilli(endTime).UTC())
}

func (t *TimeRangeQuery) Times() (startTimeArg time.Time, endTimeArg time.Time) {
	startTime, _ := strconv.ParseInt(t.TimeRange.StartTime, 10, 64)
	endTime, _ := strconv.ParseInt(t.TimeRange.EndTime, 10, 64)
	startTimeArg = time.UnixMilli(startTime).In(time.UTC)
	endTimeArg = time.UnixMilli(endTime).In(time.UTC)
	return startTimeArg, endTimeArg
}

func NewTimeRangeQuery(startTime time.Time, endTime time.Time) *TimeRangeQuery {
	return &TimeRangeQuery{TimeRange: TimeRange{StartTime: strconv.FormatInt(startTime.UnixMilli(), 10), EndTime: strconv.FormatInt(endTime.UnixMilli(), 10)}}
}

func parseTimeFromDateStr(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		log.Fatalf("parseTimeFromDateStr %v %v\n", s, err)
	}
	return t
}

func parseTimeFromTimeStr(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		log.Fatalf("parseTimeFromTimeStr %v %v\n", s, err)
	}
	return t
}

func queryMaxTimestamp(dataSourceName string, timeRangeQuery *TimeRangeQuery) time.Time {
	var where string
	var startTimeArg, endTimeArg time.Time

	if timeRangeQuery != nil {
		startTimeArg, endTimeArg = timeRangeQuery.Times()
		where = fmt.Sprintf("where timestamp between '%v' and '%v'", startTimeArg.Format(time.RFC3339), endTimeArg.Format(time.RFC3339))
		//where = fmt.Sprintf("where timestamp > '%v'", startTimeArg.Format(time.RFC3339))
	}

	query := fmt.Sprintf("select max(timestamp) from Event %v", where)

	db, err := sqlx.Open( /*"postgres"*/ "pgx", dataSourceName)
	if err != nil {
		log.Fatal(err)
	}

	maxTimestamp := time.Now().UTC()
	row := db.QueryRow(query)

	if timeRangeQuery != nil {
		err = row.Scan(&maxTimestamp)
		if err != nil {
			maxTimestamp = startTimeArg
		}
	} else {
		err = row.Scan(&maxTimestamp)
		if err != nil {
			log.Fatal(err)
		}
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}

	return maxTimestamp
}

func unsavedEventsQuery(dataSourceName string, timeRangeQuery *TimeRangeQuery, window time.Duration) (query *TimeRangeQuery, final bool) {
	maxTimestamp := queryMaxTimestamp(dataSourceName, timeRangeQuery)
	// https://github.com/MystenLabs/sui/blob/f79ce4a54cd6a9270ddf538e719d4611571363d0/sdk/typescript/src/types/events.ts#L66
	/*TimeRange: {
		// left endpoint of time interval, milliseconds since epoch, inclusive
	start_time: number;
		// right endpoint of time interval, milliseconds since epoch, exclusive
	end_time: number;
	};*/

	nextTimestamp := maxTimestamp.Add(time.Millisecond)
	//nextTimestamp := maxTimestamp

	var startTime, endTime, endWindow time.Time

	//log.Printf("latest of all events is maxTimestamp=%v %v, starting query with nextTimestamp=%v %v", maxTimestamp.UnixMilli(), maxTimestamp.UTC(), nextTimestamp.UnixMilli(), nextTimestamp.UTC())

	if timeRangeQuery != nil {
		startTimeArg, endTimeArg := timeRangeQuery.Times()

		if endTimeArg.Before(nextTimestamp) {
			log.Fatalf("end time of the time range %v is past the latest timestamp %v in the database, quitting as makes no sense to query\n", timeRangeQuery, nextTimestamp)
		}
		if endTimeArg.After(nextTimestamp) && startTimeArg.Before(nextTimestamp) {
			log.Printf("start time of the time range %v is before the latest timestamp %v in the database, will ignore the start time and query from the latest timestamp to the end of the time range\n", timeRangeQuery, nextTimestamp)
			startTime = nextTimestamp
		}
		if endTimeArg.After(nextTimestamp) && (startTimeArg.After(nextTimestamp) || startTimeArg == nextTimestamp) {
			log.Printf("both start and end times of the time range %v are after the latest timestamp %v in the database, will ignore the latest timestamp and query from start to end of the time range\n", timeRangeQuery, nextTimestamp)
			startTime = startTimeArg
		}

		endWindow = startTime.Add(window)

		if endWindow.After(endTimeArg) {
			endTime = endTimeArg
			final = true
		} else {
			endTime = endWindow
		}
	} else {
		log.Printf("will query from the latest timestamp %v in the database within a window %v\n", nextTimestamp, window)
		startTime = nextTimestamp
		endTime = startTime.Add(window)
	}

	query = NewTimeRangeQuery(startTime, endTime)

	return
}
