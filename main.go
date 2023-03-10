package main

import (
	"context"
	"encoding/json"
	"fmt"
	rpc "github.com/ybbus/jsonrpc/v3"
	"log"
	"reflect"
)

type EventPage struct {
	Data       []EventDatum `json:"data"`
	NextCursor EventID      `json:"nextCursor"`
}

type EventDatum struct {
	Timestamp int     `json:"timestamp"`
	TxDigest  string  `json:"txDigest"`
	Id        EventID `json:"id"`
	Event     Event   `json:"event"`
}

type Event struct {
	id        EventID
	timestamp int64
}

type EventID struct {
	TxDigest string `json:"txDigest"`
	EventSeq uint   `json:"eventSeq"`
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
	TxDigest          string `json:"txDigest"`
	EventSeq          uint   `json:"eventSeq"`
	Timestamp         int64  `json:"timestamp"`
	PackageId         string `json:"packageId"`
	TransactionModule string `json:"transactionModule"`
	Sender            string `json:"sender"`
	Recipient         string `json:"recipient"`
	ObjectType        string `json:"objectType"`
	ObjectId          string `json:"objectId"`
	Version           uint   `json:"version"`
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
	TxDigest  string `json:"txDigest"`
	EventSeq  uint   `json:"eventSeq"`
	Timestamp int64  `json:"timestamp"`
	Sender    string `json:"sender"`
	PackageId string `json:"packageId"`
	Version   uint   `json:"version"`
	Digest    string `json:"digest"`
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
	TxDigest          string `json:"txDigest"`
	EventSeq          uint   `json:"eventSeq"`
	Timestamp         int64  `json:"timestamp"` // todo uint64?
	PackageId         string `json:"packageId"`
	TransactionModule string `json:"transactionModule"`
	Sender            string `json:"sender"`
	Owner             string `json:"owner"`
	ChangeType        string `json:"changeType"`
	CoinType          string `json:"coinType"`
	CoinObjectId      string `json:"coinObjectId"`
	Version           uint   `json:"version"`
	Amount            int64  `json:"amount"` //todo can be negative? then int64; can be large like uint256?
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
	TxDigest          string `json:"txDigest"`
	EventSeq          uint   `json:"eventSeq"`
	Timestamp         int64  `json:"timestamp"`
	PackageId         string `json:"packageId"`
	TransactionModule string `json:"transactionModule"`
	Sender            string `json:"sender"`
	Type              string `json:"type"`
	Fields            string `json:"fields"`
	Bcs               string `json:"bcs"`
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
	TxDigest          string `json:"txDigest"`
	EventSeq          uint   `json:"eventSeq"`
	Timestamp         int64  `json:"timestamp"`
	PackageId         string `json:"packageId"`
	TransactionModule string `json:"transactionModule"`
	Sender            string `json:"sender"`
	ObjectType        string `json:"objectType"`
	ObjectId          string `json:"objectId"`
	Version           uint   `json:"version"`
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
	TxDigest          string `json:"txDigest"`
	EventSeq          uint   `json:"eventSeq"`
	Timestamp         int64  `json:"timestamp"`
	PackageId         string `json:"packageId"`
	TransactionModule string `json:"transactionModule"`
	Sender            string `json:"sender"`
	ObjectId          string `json:"objectId"`
	Version           uint   `json:"version"`
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
	TxDigest          string `json:"txDigest"`
	EventSeq          uint   `json:"eventSeq"`
	Timestamp         int64  `json:"timestamp"`
	PackageId         string `json:"packageId"`
	TransactionModule string `json:"transactionModule"`
	Sender            string `json:"sender"`
	Recipient         string `json:"recipient"`
	ObjectType        string `json:"objectType"`
	ObjectId          string `json:"objectId"`
	Version           uint   `json:"version"`
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

func toEventID(i interface{}) EventID {
	j, _ := json.Marshal(i)
	var c EventID
	json.Unmarshal(j, &c)
	return c
}

func savePublishEvent(i interface{}, id EventID, timestamp int64) {
	j, _ := json.Marshal(i)
	var o PublishEvent
	json.Unmarshal(j, &o)
	o.TxDigest = id.TxDigest
	o.EventSeq = id.EventSeq
	o.Timestamp = timestamp
	fmt.Printf("publish %v\n", o)
}

func saveTransferObjectEvent(i interface{}, id EventID, timestamp int64) {
	j, _ := json.Marshal(i)
	var o TransferObjectEvent
	json.Unmarshal(j, &o)
	o.TxDigest = id.TxDigest
	o.EventSeq = id.EventSeq
	o.Timestamp = timestamp
	fmt.Printf("transferObject %v\n", o)
}

func saveCoinBalanceChangeEvent(i interface{}, id EventID, timestamp int64) {
	j, _ := json.Marshal(i)
	var o CoinBalanceChangeEvent
	json.Unmarshal(j, &o)
	o.TxDigest = id.TxDigest
	o.EventSeq = id.EventSeq
	o.Timestamp = timestamp
	fmt.Printf("coinBalanceChange %v\n", o)
}

func main() {
	log.Println("sui-archive")

	client := rpc.NewClient("https://fullnode.devnet.sui.io")

	query := "All"
	var cursor *EventID
	//var page *EventPage

	//err := client.CallFor(context.Background(), &page, "sui_getEvents", query, cursor)
	//if err != nil {
	//	log.Fatalf("CallFor %v\n", err)
	//}
	//log.Printf("first page %v\n", page.NextCursor)
	//
	//cursor = &page.NextCursor
	//
	//for cursor != nil {
	//	err := client.CallFor(context.Background(), &page, "sui_getEvents", query, cursor)
	//	if err != nil {
	//		log.Fatalf("CallFor %v\n", err)
	//	}
	//	log.Printf("page %v\n", page.NextCursor)
	//}

	response, err := client.Call(context.Background(), "sui_getEvents", query, cursor)
	if err != nil {
		log.Fatalf("CallFor %v\n", err)
	}

	if response.Error != nil {
		// rpc error handling goes here
		// check response.Error.Code, response.Error.Message and optional response.Error.Data
		log.Fatalf("rpc response error %v\n", response.Error)
	}

	iterResponse := reflect.ValueOf(response.Result).MapRange()
	for iterResponse.Next() {
		keyResponse := iterResponse.Key().String()
		interfaceResponse := iterResponse.Value().Interface()

		switch keyResponse {
		case "data":
			arrayData := iterResponse.Value().Interface().([]interface{})

			for _, datum := range arrayData {
				iterDatum := reflect.ValueOf(datum).MapRange()
				for iterDatum.Next() {
					keyDatum := iterDatum.Key().String()
					interfaceDatum := iterDatum.Value().Interface()

					var id EventID
					var timestamp int64

					switch keyDatum {
					case "id":
						id = toEventID(interfaceDatum)
					case "timestamp":
						v := iterDatum.Value().Interface()
						timestamp, _ = v.(json.Number).Int64()
					case "event":
						iterEvent := reflect.ValueOf(interfaceDatum).MapRange()
						for iterEvent.Next() {
							keyEvent := iterEvent.Key().String()
							interfaceEvent := iterEvent.Value().Interface()

							switch keyEvent {
							case "transferObject":
								saveTransferObjectEvent(interfaceEvent, id, timestamp)
							case "publish":
								savePublishEvent(interfaceEvent, id, timestamp)
							case "coinBalanceChange":
								saveCoinBalanceChangeEvent(interfaceEvent, id, timestamp)
							}
						}
					}
				}
			}
		case "nextCursor":
			*cursor = toEventID(interfaceResponse)
			fmt.Printf("nextCursor %v\n", cursor)
		}

	}

	//v, ok := response.Result.(map[string]interface{})
	//if !ok {
	//	log.Fatalf("%v\n", ok)
	//}
	//for _, s := range v {
	//	fmt.Printf("Value: %v\n", s)
	//}

	//v := reflect.ValueOf(response.Result)
	//
	//if v.Kind() == reflect.Map {
	//	for _, key := range v.MapKeys() {
	//		key.String()
	//		strct := v.MapIndex(key)
	//		fmt.Println(key.Interface(), strct.Interface())
	//	}
	//}

	//	err = response.GetObject(&page) // expects a rpc-object result value like: {"id": 123, "name": "alex", "age": 33}
	//if err != nil || page == nil {
	//	// some error on json unmarshal level or json result field was null
	//	log.Fatalf("rpc cannot get object %v\n", err)
	//}

}

//type Event struct {
//	Type                  string           `json:"type"`
//	RawContent            *json.RawMessage `json:"content"`
//	content               interface{}      `json:"-"`
//	unmarshalContentMutex sync.Mutex       `json:"-"`
//}
//
//func (p *Event) Content() (interface{}, error) {
//	if p.RawContent == nil {
//		return p.content, nil
//	}
//
//	p.unmarshalContentMutex.Lock()
//	if p.RawContent != nil {
//		rawContent := p.RawContent
//		p.RawContent = nil
//
//		switch p.Type {
//		case "transferObject":
//			p.content = &TransferObjectEvent{}
//		case "publishObject":
//			p.content = &PublishEvent{}
//		}
//
//		if err := json.Unmarshal([]byte(*rawContent), &p.content); err != nil {
//			return nil, err
//		}
//	}
//	p.unmarshalContentMutex.Unlock()
//
//	return p.content, nil
//}
//
//func (p *Event) MarshalJSON() ([]byte, error) {
//	rawData, err := json.Marshal(p.content)
//	if err != nil {
//		return nil, err
//	}
//	rawMessage := json.RawMessage(rawData)
//	p.RawContent = &rawMessage
//
//	return json.Marshal(*p) // Important, using p here would end in recursion
//}
//
//var _ json.Marshaler = (*Event)(nil)
