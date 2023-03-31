package main

import (
	"encoding/json"
	"github.com/jmoiron/sqlx"
	"log"
	"reflect"
	"time"
)

type Event interface {
	SetId(id EventID)
	SetTimestamp(timestamp int64)
	InsertQuery() string
	GetTimestamp() time.Time
}

type EventID struct {
	TxDigest string `json:"txDigest"`
	EventSeq int    `json:"eventSeq"`
}

type BaseEvent struct {
	EventID
	Timestamp   int64 `json:"timestamp"`
	TimestampDb time.Time
}

func (t *BaseEvent) SetId(id EventID) {
	t.TxDigest = id.TxDigest
	t.EventSeq = id.EventSeq
}

func (t *BaseEvent) SetTimestamp(timestamp int64) {
	t.Timestamp = timestamp
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal(err)
	}
	t.TimestampDb = time.UnixMilli(timestamp).In(loc)
}

func (t *BaseEvent) GetTimestamp() time.Time {
	return t.TimestampDb
}

type EventTypeQuery struct {
	EventType string `json:"EventType"`
}

func NewEventTypeQuery(s string) *EventTypeQuery {
	return &EventTypeQuery{EventType: s}
}

type EventResponseResult struct {
	NextCursor EventID      `json:"nextCursor"`
	Data       []EventDatum `json:"data"`
}

func (t *EventResponseResult) Save(dataSourceName string) {
	eventMap := make(map[reflect.Type][]Event, 7)

	for _, d := range t.Data {
		eventType := d.GetEventType()
		if eventMap[eventType] == nil {
			eventMap[eventType] = make([]Event, 0)
		}
		e := d.GetEvent()
		e.SetId(d.Id)
		e.SetTimestamp(d.Timestamp)
		eventMap[eventType] = append(eventMap[eventType], e)
	}

	db, err := sqlx.Open( /*"postgres"*/ "pgx", dataSourceName)
	if err != nil {
		log.Fatal(err)
	}

	for eventType, eventArray := range eventMap {
		e := eventArray[len(eventArray)-1]
		eventTimestamp := e.GetTimestamp()
		insertQuery := e.InsertQuery() //todo get InsertQuery in the constructor from an instance of the eventType; prepared statement?

		_, err := db.NamedExec(insertQuery, eventArray)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("inserted %v %s with eventTimestamp %v", len(eventArray), eventType, eventTimestamp.UTC())
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

type EventDatum struct {
	Timestamp int64       `json:"timestamp"`
	Id        EventID     `json:"id"`
	Event     EventChoice `json:"event"`
}

func (d *EventDatum) GetEvent() Event {
	if d.Event.MoveEvent != nil {
		return d.Event.MoveEvent
	} else if d.Event.TransferObject != nil {
		return d.Event.TransferObject
	} else if d.Event.Publish != nil {
		return d.Event.Publish
	} else if d.Event.CoinBalanceChange != nil {
		return d.Event.CoinBalanceChange
	} else if d.Event.MutateObject != nil {
		return d.Event.MutateObject
	} else if d.Event.DeleteObject != nil {
		return d.Event.DeleteObject
	} else if d.Event.NewObject != nil {
		return d.Event.NewObject
	} else {
		log.Fatalf("unknown event %v", d.Event)
		return nil
	}
}

func (d *EventDatum) GetEventType() reflect.Type {
	return reflect.TypeOf(d.GetEvent())
}

type EventChoice struct {
	Publish           *PublishEvent           `json:"publish,omitempty"`
	TransferObject    *TransferObjectEvent    `json:"transferObject,omitempty"`
	CoinBalanceChange *CoinBalanceChangeEvent `json:"coinBalanceChange,omitempty"`
	MoveEvent         *MoveEvent              `json:"moveEvent,omitempty"`
	MutateObject      *MutateObjectEvent      `json:"mutateObject,omitempty"`
	DeleteObject      *DeleteObjectEvent      `json:"deleteObject,omitempty"`
	NewObject         *NewObjectEvent         `json:"newObject,omitempty"`
}

var EventNames = []string{"PublishEvent", "TransferObjectEvent", "CoinBalanceChangeEvent", "MoveEvent", "MutateObjectEvent", "DeleteObjectEvent", "NewObjectEvent"}

/*
	export const TransferObjectEvent = object({
	  packageId: ObjectId,
	  transactionModule: string(),
	  sender: SuiAddress,
	  recipient: ObjectOwner,
	  objectType: string(),
	  objectId: ObjectId,
	  version: SequenceNumber,
	});
*/
type TransferObjectEvent struct {
	BaseEvent
	PackageId         string          `json:"packageId"`
	TransactionModule string          `json:"transactionModule"`
	Sender            string          `json:"sender"`
	Recipient         json.RawMessage `json:"recipient"`
	ObjectType        string          `json:"objectType"`
	ObjectId          string          `json:"objectId"`
	Version           int             `json:"version"`
}

func (t *TransferObjectEvent) InsertQuery() string {
	return "insert into TransferObjectEvent (txDigest, eventSeq, timestamp, packageId, transactionModule, sender, recipient, objectType, objectId, version) values (:txdigest, :eventseq, :timestampdb, :packageid, :transactionmodule, :sender, :recipient, :objecttype, :objectid, :version) on conflict on constraint TransferObjectEvent_pkey do nothing"
}

/*
	export const PublishEvent = object({
	  sender: SuiAddress,
	  packageId: ObjectId,
	  version: optional(number()),
	  digest: optional(string()),
	});
*/
type PublishEvent struct {
	BaseEvent
	Sender    string `json:"sender"`
	PackageId string `json:"packageId"`
	Version   int    `json:"version"`
	Digest    string `json:"digest"`
}

func (t *PublishEvent) InsertQuery() string {
	return "insert into PublishEvent (txDigest, eventSeq, timestamp, sender, packageId, version, digest) values (:txdigest, :eventseq, :timestampdb, :sender, :packageid, :version, :digest) on conflict on constraint PublishEvent_pkey do nothing"
}

/*
	export const CoinBalanceChangeEvent = object({
	  packageId: ObjectId,
	  transactionModule: string(),
	  sender: SuiAddress,
	  owner: ObjectOwner,
	  changeType: BalanceChangeType,
	  coinType: string(),
	  coinObjectId: ObjectId,
	  version: SequenceNumber,
	  amount: number(),
	});
*/
type CoinBalanceChangeEvent struct {
	BaseEvent
	PackageId         string          `json:"packageId"`
	TransactionModule string          `json:"transactionModule"`
	Sender            string          `json:"sender"`
	Owner             json.RawMessage `json:"owner"`
	ChangeType        string          `json:"changeType"`
	CoinType          string          `json:"coinType"`
	CoinObjectId      string          `json:"coinObjectId"`
	Version           int             `json:"version"`
	Amount            json.Number     `json:"amount"`
	// todo use uint64? can it be large like ethereum's uint256?  Unmarshal json: cannot unmarshal number 18446744073709551615 into Go struct field CoinBalanceChangeEvent.amount of type int64; see https://github.com/xitongsys/parquet-go/issues/419
	//Amount            int64  `json:"amount" parquet:"name=amount, type=INT64, convertedtype=UINT_64"`
}

func (t *CoinBalanceChangeEvent) InsertQuery() string {
	return "insert into CoinBalanceChangeEvent (txDigest, eventSeq, timestamp, packageId, transactionModule, sender, owner, changeType, coinType, coinObjectId, version, amount) values (:txdigest, :eventseq, :timestampdb, :packageid, :transactionmodule, :sender, :owner, :changetype, :cointype, :coinobjectid, :version, :amount) on conflict on constraint CoinBalanceChangeEvent_pkey do nothing"
}

/*
	export const MoveEvent = object({
	  packageId: ObjectId,
	  transactionModule: string(),
	  sender: SuiAddress,
	  type: string(),
	  fields: record(string(), any()),
	  bcs: string(),
	});
*/
type MoveEvent struct {
	BaseEvent
	PackageId         string          `json:"packageId"`
	TransactionModule string          `json:"transactionModule"`
	Sender            string          `json:"sender"`
	Type              string          `json:"type"`
	Fields            json.RawMessage `json:"fields"`
	Bcs               string          `json:"bcs"`
}

func (t *MoveEvent) InsertQuery() string {
	return "insert into MoveEvent (txDigest, eventSeq, timestamp, packageId, transactionModule, sender, type, fields, bcs) values (:txdigest, :eventseq, :timestampdb, :packageid, :transactionmodule, :sender, :type, :fields, :bcs) on conflict on constraint MoveEvent_pkey do nothing"
}

/*
export const MutateObjectEvent = object({
packageId: ObjectId,
transactionModule: string(),
sender: SuiAddress,
objectType: string(),
objectId: ObjectId,
version: SequenceNumber,
});
*/
type MutateObjectEvent struct {
	BaseEvent
	PackageId         string `json:"packageId"`
	TransactionModule string `json:"transactionModule"`
	Sender            string `json:"sender"`
	ObjectType        string `json:"objectType"`
	ObjectId          string `json:"objectId"`
	Version           int    `json:"version"`
}

func (t *MutateObjectEvent) InsertQuery() string {
	return "insert into MutateObjectEvent (txDigest, eventSeq, timestamp, packageId, transactionModule, sender, objectType, objectId, version) values (:txdigest, :eventseq, :timestampdb, :packageid, :transactionmodule, :sender, :objecttype, :objectid, :version) on conflict on constraint MutateObjectEvent_pkey do nothing"
}

/*
	export const DeleteObjectEvent = object({
	  packageId: ObjectId,
	  transactionModule: string(),
	  sender: SuiAddress,
	  objectId: ObjectId,
	  version: SequenceNumber,
	});
*/
type DeleteObjectEvent struct {
	BaseEvent
	PackageId         string `json:"packageId"`
	TransactionModule string `json:"transactionModule"`
	Sender            string `json:"sender"`
	ObjectId          string `json:"objectId"`
	Version           int    `json:"version"`
}

func (t *DeleteObjectEvent) InsertQuery() string {
	return "insert into DeleteObjectEvent (txDigest, eventSeq, timestamp, packageId, transactionModule, sender, objectId, version) values (:txdigest, :eventseq, :timestampdb, :packageid, :transactionmodule, :sender, :objectid, :version) on conflict on constraint DeleteObjectEvent_pkey do nothing"
}

/*
	export const NewObjectEvent = object({
	  packageId: ObjectId,
	  transactionModule: string(),
	  sender: SuiAddress,
	  recipient: ObjectOwner,
	  objectType: string(),
	  objectId: ObjectId,
	  version: SequenceNumber,
	});
*/
type NewObjectEvent struct {
	BaseEvent
	PackageId         string          `json:"packageId"`
	TransactionModule string          `json:"transactionModule"`
	Sender            string          `json:"sender"`
	Recipient         json.RawMessage `json:"recipient"`
	ObjectType        string          `json:"objectType"`
	ObjectId          string          `json:"objectId"`
	Version           int             `json:"version"`
}

func (t *NewObjectEvent) InsertQuery() string {
	return "insert into NewObjectEvent (txDigest, eventSeq, timestamp, packageId, transactionModule, sender, recipient, objectType, objectId, version) values (:txdigest, :eventseq, :timestampdb, :packageid, :transactionmodule, :sender, :recipient, :objecttype, :objectid, :version) on conflict on constraint NewObjectEvent_pkey do nothing"
}

/*
export const EpochChangeEvent = union([bigint(), number()]);
*/

/*
export const CheckpointEvent = union([bigint(), number()]);
*/

/*
export const SuiEvent = union([
  object({ moveEvent: MoveEvent }),
  object({ publish: PublishEvent }),
  object({ coinBalanceChange: CoinBalanceChangeEvent }),
  object({ transferObject: TransferObjectEvent }),
  object({ mutateObject: MutateObjectEvent }),
  object({ deleteObject: DeleteObjectEvent }),
  object({ newObject: NewObjectEvent }),
  object({ epochChange: EpochChangeEvent }),
  object({ checkpoint: CheckpointEvent }),
]);
*/
