package populators

import (
	"encoding/json"
	"fmt"
	"github.com/adamhei/honors-project/exchanges/models"
	"gopkg.in/mgo.v2"
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

	now := time.Now()
	for indexTime := time.Unix(0, int64(firstTradeTimestampMs*time.Millisecond)); indexTime.Before(now); {

		orders := getTradeHistory(indexTime)

		bulkInsert.Insert(orders)
		fmt.Println("Prepared", len(orders), "records")
		// Offset by 1 to exclude last trade
		indexTime = time.Unix(0, orders[0].TimestampMs*int64(time.Millisecond) + 1)
	}

	res, err := bulkInsert.Run()
	if err != nil {
		fmt.Println("Couldn't perform batch insert")
		log.Fatal(err)
	}

	fmt.Println("Modified", res.Modified, "records")
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
