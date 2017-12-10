package populators

import (
	"encoding/json"
	"fmt"
	"github.com/adamhei/honorsproject/exchanges/models"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"net/http"
	"time"
)

const historyUrl = "https://api.gemini.com/v1/trades/btcusd?since=%d&limit_trades=500"
const collectionName = "gemini"
const firstTradeTimestampMs = 1444311607801

func Populate(db *mgo.Database) {
	collection := db.C(collectionName)
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
				log.Fatal(err)
			}
			log.Println("Matched", res.Matched, "docs and modified", res.Modified)
			bulkInsert = collection.Bulk()
		}
	}
}

func getTradeHistory(from time.Time) []models.GeminiOrder {
	formattedUrl := fmt.Sprintf(historyUrl, from.Unix())

	resp, err := http.Get(formattedUrl)
	if err != nil {
		log.Fatal(err)
	}

	orders := make([]models.GeminiOrder, 0)
	err = json.NewDecoder(resp.Body).Decode(&orders)
	resp.Body.Close()

	if err != nil {
		log.Fatal(err)
	}

	// Be considerate
	time.Sleep(500 * time.Millisecond)

	return orders
}

func getTimestampMs(coll *mgo.Collection, byMostRecent bool) int64 {
	query := ""
	if byMostRecent {
		query = "-timestampms"
	} else {
		query = "timestampms"
	}

	res := new(models.GeminiOrder)
	coll.Find(bson.M{}).Sort(query).One(&res)
	if res.TimestampMs != 0 {
		return res.TimestampMs + 1
	}
	return firstTradeTimestampMs
}

func toInterfaceSlice(orders []models.GeminiOrder) []interface{} {
	arr := make([]interface{}, len(orders))
	for i, v := range orders {
		arr[i] = v
	}
	return arr
}
