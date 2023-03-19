package main

import (
	"encoding/json"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"log"
	"reflect"
	"time"
)

type EventSaver interface {
	Start()
	Save(interfaceEvent interface{}, id EventID, timestamp int64)
	Commit()
	Stop()
	EventType() reflect.Type
}

type BaseEventSaver struct {
	name      string
	eventType reflect.Type
}

func NewBaseEventSaver(name string) *BaseEventSaver {
	var t reflect.Type
	switch name {
	case "transferObject":
		t = reflect.TypeOf(TransferObjectEvent{})
	case "publish":
		t = reflect.TypeOf(PublishEvent{})
	case "coinBalanceChange":
		t = reflect.TypeOf(CoinBalanceChangeEvent{})
	case "moveEvent":
		t = reflect.TypeOf(MoveEvent{})
	case "mutateObject":
		t = reflect.TypeOf(MutateObjectEvent{})
	case "deleteObject":
		t = reflect.TypeOf(DeleteObjectEvent{})
	case "newObject":
		t = reflect.TypeOf(NewObjectEvent{})
	}
	return &BaseEventSaver{name: name, eventType: t}
}

func (t *BaseEventSaver) Parse(interfaceEvent interface{}, id EventID, timestamp int64) (e Event) {
	j, _ := json.Marshal(interfaceEvent)

	i := reflect.New(t.eventType).Interface()
	e = i.(Event)
	err := json.Unmarshal(j, &e)
	if err != nil {
		log.Fatalf("Unmarshal %v %v", id, err)
		//log.Printf("Unmarshal %v %v", id, err)
	}
	e.SetId(id)
	e.SetTimestamp(timestamp)
	//log.Printf("%v %v\n", t.name, e)

	return e
}

func (t *BaseEventSaver) EventType() reflect.Type {
	return t.eventType
}

type FileEventSaver struct {
	BaseEventSaver
	filenameSuffix  string
	folder          string
	parquetWriter   *writer.ParquetWriter
	parquetFile     source.ParquetFile
	parquetFilename string
}

func NewFileEventSaver(name string, filenameSuffix string, folder string) *FileEventSaver {
	return &FileEventSaver{BaseEventSaver: *NewBaseEventSaver(name), filenameSuffix: filenameSuffix, folder: folder}
}

func (t *FileEventSaver) Start() {
	var err error

	t.parquetFilename = fmt.Sprintf("%s/%s%s.parquet", t.folder, t.name, t.filenameSuffix)
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

func (t *FileEventSaver) Save(interfaceEvent interface{}, id EventID, timestamp int64) {
	err := t.parquetWriter.Write(t.Parse(interfaceEvent, id, timestamp))
	if err != nil {
		log.Fatalf("Write %v", err)
	}
}

func (t *FileEventSaver) Commit() {
}

func (t *FileEventSaver) Stop() {
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

type DatabaseEventSaver struct {
	BaseEventSaver
	dataSourceName string
	db             *sqlx.DB
	events         []Event
}

func NewDatabaseEventSaver(name string, dataSourceName string) *DatabaseEventSaver {
	return &DatabaseEventSaver{BaseEventSaver: *NewBaseEventSaver(name), dataSourceName: dataSourceName}
}

func (t *DatabaseEventSaver) Start() {
	var err error

	t.db, err = sqlx.Open( /*"postgres"*/ "pgx", t.dataSourceName)
	if err != nil {
		log.Fatal(err)
	}
	//t.db.SetMaxOpenConns(8)
}

func (t *DatabaseEventSaver) Save(interfaceEvent interface{}, id EventID, timestamp int64) {
	t.events = append(t.events, t.Parse(interfaceEvent, id, timestamp))
}

func (t *DatabaseEventSaver) Commit() {
	var eventTimestamp time.Time

	//log.Printf("committing %v events", t.eventType.Name())

	if len(t.events) > 0 {
		e := t.events[len(t.events)-1]
		eventTimestamp = e.GetTimestamp()
		insertQuery := e.InsertQuery() //todo get InsertQuery in the constructor from an instance of the eventType; prepared statement?

		_, err := t.db.NamedExec(insertQuery, t.events)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("inserted %v %v with eventTimestamp %v", len(t.events), t.eventType.Name(), eventTimestamp.UTC())

	t.events = []Event{}
}

func (t *DatabaseEventSaver) Stop() {
	err := t.db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
