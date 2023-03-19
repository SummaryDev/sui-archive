package main

import (
	"encoding/json"
	"log"
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

func NewEventID(i interface{}) *EventID {
	j, _ := json.Marshal(i)
	o := &EventID{}
	json.Unmarshal(j, o) //todo fail on this?
	return o
}

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
	TxDigest          string `json:"txDigest" parquet:"name=txDigest, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	EventSeq          int    `json:"eventSeq" parquet:"name=eventSeq, type=INT32, convertedtype=UINT_32"`
	Timestamp         int64  `json:"timestamp" parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	TimestampDb       time.Time
	PackageId         string `json:"packageId" parquet:"name=packageId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionModule string `json:"transactionModule" parquet:"name=transactionModule, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Sender            string `json:"sender" parquet:"name=sender, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Recipient         string `json:"recipient" parquet:"name=recipient, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	ObjectType        string `json:"objectType" parquet:"name=objectType, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ObjectId          string `json:"objectId" parquet:"name=objectId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Version           int    `json:"version" parquet:"name=version, type=INT32, convertedtype=UINT_32"`
}

func (t *TransferObjectEvent) SetId(id EventID) {
	t.TxDigest = id.TxDigest
	t.EventSeq = id.EventSeq
}

func (t *TransferObjectEvent) SetTimestamp(timestamp int64) {
	t.Timestamp = timestamp
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal(err)
	}
	t.TimestampDb = time.UnixMilli(timestamp).In(loc)
}

func (t *TransferObjectEvent) GetTimestamp() time.Time {
	return t.TimestampDb
}

func (t *TransferObjectEvent) InsertQuery() string {
	return "insert into TransferObjectEvent (txDigest, eventSeq, timestamp, packageId, transactionModule, sender, recipient, objectType, objectId, version) values (:txdigest, :eventseq, :timestampdb, :packageid, :transactionmodule, :sender, :recipient, :objecttype, :objectid, :version)"
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
	TxDigest    string `json:"txDigest" parquet:"name=txDigest, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	EventSeq    int    `json:"eventSeq" parquet:"name=eventSeq, type=INT32, convertedtype=UINT_32"`
	Timestamp   int64  `json:"timestamp" parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	TimestampDb time.Time
	Sender      string `json:"sender" parquet:"name=sender, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	PackageId   string `json:"packageId" parquet:"name=packageId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Version     int    `json:"version" parquet:"name=version, type=INT32, convertedtype=UINT_32"`
	Digest      string `json:"digest" parquet:"name=digest, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
}

func (t *PublishEvent) SetId(id EventID) {
	t.TxDigest = id.TxDigest
	t.EventSeq = id.EventSeq
}

func (t *PublishEvent) SetTimestamp(timestamp int64) {
	t.Timestamp = timestamp
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal(err)
	}
	t.TimestampDb = time.UnixMilli(timestamp).In(loc)
}

func (t *PublishEvent) GetTimestamp() time.Time {
	return t.TimestampDb
}

func (t *PublishEvent) InsertQuery() string {
	return "insert into PublishEvent (txDigest, eventSeq, timestamp, sender, packageId, version, digest) values (:txdigest, :eventseq, :timestampdb, :sender, :packageid, :version, :digest)"
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
	TxDigest          string `json:"txDigest" parquet:"name=txDigest, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	EventSeq          int    `json:"eventSeq" parquet:"name=eventSeq, type=INT32, convertedtype=UINT_32"`
	Timestamp         int64  `json:"timestamp" parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	TimestampDb       time.Time
	PackageId         string      `json:"packageId" parquet:"name=packageId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionModule string      `json:"transactionModule" parquet:"name=transactionModule, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Sender            string      `json:"sender" parquet:"name=sender, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Owner             string      `json:"owner" parquet:"name=owner, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	ChangeType        string      `json:"changeType" parquet:"name=changeType, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CoinType          string      `json:"coinType" parquet:"name=coinType, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CoinObjectId      string      `json:"coinObjectId" parquet:"name=coinObjectId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Version           int         `json:"version" parquet:"name=version, type=INT32, convertedtype=UINT_32"`
	Amount            json.Number `json:"amount" parquet:"name=amount, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	// todo use uint64? can it be large like ethereum's uint256?  Unmarshal json: cannot unmarshal number 18446744073709551615 into Go struct field CoinBalanceChangeEvent.amount of type int64; see https://github.com/xitongsys/parquet-go/issues/419
	//Amount            int64  `json:"amount" parquet:"name=amount, type=INT64, convertedtype=UINT_64"`
}

func (t *CoinBalanceChangeEvent) SetId(id EventID) {
	t.TxDigest = id.TxDigest
	t.EventSeq = id.EventSeq
}

func (t *CoinBalanceChangeEvent) SetTimestamp(timestamp int64) {
	t.Timestamp = timestamp
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal(err)
	}
	t.TimestampDb = time.UnixMilli(timestamp).In(loc)
}

func (t *CoinBalanceChangeEvent) GetTimestamp() time.Time {
	return t.TimestampDb
}

func (t *CoinBalanceChangeEvent) InsertQuery() string {
	return "insert into CoinBalanceChangeEvent (txDigest, eventSeq, timestamp, packageId, transactionModule, sender, owner, changeType, coinType, coinObjectId, version, amount) values (:txdigest, :eventseq, :timestampdb, :packageid, :transactionmodule, :sender, :owner, :changetype, :cointype, :coinobjectid, :version, :amount)"
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
	TxDigest          string `json:"txDigest" parquet:"name=txDigest, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	EventSeq          int    `json:"eventSeq" parquet:"name=eventSeq, type=INT32, convertedtype=UINT_32"`
	Timestamp         int64  `json:"timestamp" parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	TimestampDb       time.Time
	PackageId         string `json:"packageId" parquet:"name=packageId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionModule string `json:"transactionModule" parquet:"name=transactionModule, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Sender            string `json:"sender" parquet:"name=sender, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Type              string `json:"type" parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Fields            string `json:"fields" parquet:"name=fields, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Bcs               string `json:"bcs" parquet:"name=bcs, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
}

func (t *MoveEvent) SetId(id EventID) {
	t.TxDigest = id.TxDigest
	t.EventSeq = id.EventSeq
}

func (t *MoveEvent) SetTimestamp(timestamp int64) {
	t.Timestamp = timestamp
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal(err)
	}
	t.TimestampDb = time.UnixMilli(timestamp).In(loc)
}

func (t *MoveEvent) GetTimestamp() time.Time {
	return t.TimestampDb
}

func (t *MoveEvent) InsertQuery() string {
	return "insert into MoveEvent (txDigest, eventSeq, timestamp, packageId, transactionModule, sender, type, fields, bcs) values (:txdigest, :eventseq, :timestampdb, :packageid, :transactionmodule, :sender, :type, :fields, :bcs)"
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
	TxDigest          string `json:"txDigest" parquet:"name=txDigest, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	EventSeq          int    `json:"eventSeq" parquet:"name=eventSeq, type=INT32, convertedtype=UINT_32"`
	Timestamp         int64  `json:"timestamp" parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	TimestampDb       time.Time
	PackageId         string `json:"packageId" parquet:"name=packageId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionModule string `json:"transactionModule" parquet:"name=transactionModule, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Sender            string `json:"sender" parquet:"name=sender, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	ObjectType        string `json:"objectType" parquet:"name=objectType, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ObjectId          string `json:"objectId" parquet:"name=objectId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Version           int    `json:"version" parquet:"name=version, type=INT32, convertedtype=UINT_32"`
}

func (t *MutateObjectEvent) SetId(id EventID) {
	t.TxDigest = id.TxDigest
	t.EventSeq = id.EventSeq
}

func (t *MutateObjectEvent) SetTimestamp(timestamp int64) {
	t.Timestamp = timestamp
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal(err)
	}
	t.TimestampDb = time.UnixMilli(timestamp).In(loc)
}

func (t *MutateObjectEvent) GetTimestamp() time.Time {
	return t.TimestampDb
}

func (t *MutateObjectEvent) InsertQuery() string {
	return "insert into MutateObjectEvent (txDigest, eventSeq, timestamp, packageId, transactionModule, sender, objectType, objectId, version) values (:txdigest, :eventseq, :timestampdb, :packageid, :transactionmodule, :sender, :objecttype, :objectid, :version)"
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
	TxDigest          string `json:"txDigest" parquet:"name=txDigest, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	EventSeq          int    `json:"eventSeq" parquet:"name=eventSeq, type=INT32, convertedtype=UINT_32"`
	Timestamp         int64  `json:"timestamp" parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	TimestampDb       time.Time
	PackageId         string `json:"packageId" parquet:"name=packageId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionModule string `json:"transactionModule" parquet:"name=transactionModule, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Sender            string `json:"sender" parquet:"name=sender, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	ObjectId          string `json:"objectId" parquet:"name=objectId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Version           int    `json:"version" parquet:"name=version, type=INT32, convertedtype=UINT_32"`
}

func (t *DeleteObjectEvent) SetId(id EventID) {
	t.TxDigest = id.TxDigest
	t.EventSeq = id.EventSeq
}

func (t *DeleteObjectEvent) SetTimestamp(timestamp int64) {
	t.Timestamp = timestamp
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal(err)
	}
	t.TimestampDb = time.UnixMilli(timestamp).In(loc)
}

func (t *DeleteObjectEvent) GetTimestamp() time.Time {
	return t.TimestampDb
}

func (t *DeleteObjectEvent) InsertQuery() string {
	return "insert into DeleteObjectEvent (txDigest, eventSeq, timestamp, packageId, transactionModule, sender, objectId, version) values (:txdigest, :eventseq, :timestampdb, :packageid, :transactionmodule, :sender, :objectid, :version)"
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
	TxDigest          string `json:"txDigest" parquet:"name=txDigest, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	EventSeq          int    `json:"eventSeq" parquet:"name=eventSeq, type=INT32, convertedtype=UINT_32"`
	Timestamp         int64  `json:"timestamp" parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	TimestampDb       time.Time
	PackageId         string `json:"packageId" parquet:"name=packageId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionModule string `json:"transactionModule" parquet:"name=transactionModule, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Sender            string `json:"sender" parquet:"name=sender, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Recipient         string `json:"recipient" parquet:"name=recipient, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	ObjectType        string `json:"objectType" parquet:"name=objectType, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ObjectId          string `json:"objectId" parquet:"name=objectId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Version           int    `json:"version" parquet:"name=version, type=INT32, convertedtype=UINT_32"`
}

func (t *NewObjectEvent) SetId(id EventID) {
	t.TxDigest = id.TxDigest
	t.EventSeq = id.EventSeq
}

func (t *NewObjectEvent) SetTimestamp(timestamp int64) {
	t.Timestamp = timestamp
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal(err)
	}
	t.TimestampDb = time.UnixMilli(timestamp).In(loc)
}

func (t *NewObjectEvent) GetTimestamp() time.Time {
	return t.TimestampDb
}

func (t *NewObjectEvent) InsertQuery() string {
	return "insert into NewObjectEvent (txDigest, eventSeq, timestamp, packageId, transactionModule, sender, recipient, objectType, objectId, version) values (:txdigest, :eventseq, :timestampdb, :packageid, :transactionmodule, :sender, :recipient, :objecttype, :objectid, :version)"
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
