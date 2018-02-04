package main

import (
	"github.com/adamhei/historicaldata/trademodels"
	"github.com/adamhei/historicaldata/populators"
	"gopkg.in/mgo.v2"
	"log"
	"time"
)

func main() {
	mgoDialInfo := &mgo.DialInfo{
		Addrs:    []string{trademodels.DbUrl},
		Timeout:  1 * time.Hour,
		Database: trademodels.AUTHDB,
		Username: trademodels.USERNAME,
		Password: trademodels.PASSWORD,
	}
	sesh, err := mgo.DialWithInfo(mgoDialInfo)
	defer sesh.Close()

	if err != nil {
		log.Println("Could not connect to DB")
		panic(err)
	}

	db := sesh.DB(trademodels.DbName)

	populators.Populate(db)
}
