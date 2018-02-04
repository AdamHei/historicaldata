package populators

import (
	"encoding/json"
	"fmt"
	"github.com/adamhei/historicaldata/trademodels"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"net/http"
	"time"
)

const historyUrl = "https://api.gemini.com/v1/trades/btcusd?since=%d&limit_trades=500"
const firstTradeTimestampMs = 1444311607801

func Populate(db *mgo.Database) {
	collection := db.C(trademodels.GeminiCollection)
	bulkInsert := collection.Bulk()

	earliestTimestampMs := getTimestampMs(collection, false)
	latestTimestampMS := getTimestampMs(collection, true)
	log.Println("Earliest trade:", time.Unix(0, earliestTimestampMs*int64(time.Millisecond)))
	log.Println("Latest trade:", time.Unix(0, latestTimestampMS*int64(time.Millisecond)))

	now := time.Now()
	requests := 0
	for indexTime := time.Unix(0, latestTimestampMS*int64(time.Millisecond)); indexTime.Before(now); {
		log.Println(indexTime)
		orders := getTradeHistory(indexTime)
		bulkInsert.Upsert(toInterfaceSlice(orders)...)

		// Offset by 1 to exclude last trade
		indexTime = time.Unix(0, orders[0].TimestampMs*int64(time.Millisecond)+1)

		requests++
		// After every 2 requests, we'll have 1000 orders, the max number of inserts MongoDB supports
		if requests%2 == 0 {
			res, err := bulkInsert.Run()
			if err != nil {
				log.Println("Couldn't perform batch insert")
				panic(err)
			}
			log.Println("Matched", res.Matched, "docs and modified", res.Modified)
			bulkInsert = collection.Bulk()
		}
	}
}

func getTradeHistory(from time.Time) []trademodels.GeminiOrder {
	formattedUrl := fmt.Sprintf(historyUrl, from.Unix())

	resp, err := http.Get(formattedUrl)
	if err != nil {
		log.Println("Could not perform GET request with ", historyUrl)
		log.Fatal(err)
	}

	orders := make([]trademodels.GeminiOrder, 0)
	errResp := new(trademodels.GeminiError)
	decoder := json.NewDecoder(resp.Body)

	if resp.StatusCode == 200 {
		err = decoder.Decode(&orders)

		if err != nil {
			log.Println(fmt.Sprintf("Gemini response from %s was not array of Gemini Orders", historyUrl))
			log.Fatal(err)
		}
	} else {
		err = decoder.Decode(&errResp)

		if err != nil {
			log.Println(fmt.Sprintf("Gemini error with code %d from %s was not a GeminiError", resp.StatusCode, historyUrl))
			log.Fatal(err)
		}
		log.Println("Gemini error:")
		log.Println("Result:", errResp.Result)
		log.Println("Reason:", errResp.Reason)
		log.Fatal("Message:", errResp.Message)
	}

	// Be considerate
	time.Sleep(1 * time.Second)

	return orders
}

func getTimestampMs(coll *mgo.Collection, byMostRecent bool) int64 {
	query := ""
	if byMostRecent {
		query = "-timestampms"
	} else {
		query = "timestampms"
	}

	res := new(trademodels.GeminiOrder)
	coll.Find(bson.M{}).Sort(query).One(&res)
	if res.TimestampMs != 0 {
		return res.TimestampMs + 1
	}
	return firstTradeTimestampMs
}

func toInterfaceSlice(orders []trademodels.GeminiOrder) []interface{} {
	arr := make([]interface{}, len(orders))
	for i, v := range orders {
		arr[i] = v
	}
	return arr
}
