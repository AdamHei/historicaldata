package main

import (
	"github.com/adamhei/historicaldata/models"
	"github.com/adamhei/historicaldata/populators"
	"gopkg.in/mgo.v2"
	"log"
	"time"
)

func main() {
	mgoDialInfo := &mgo.DialInfo{
		Addrs:    []string{models.DbUrl},
		Timeout:  1 * time.Hour,
		Database: models.AUTHDB,
		Username: models.USERNAME,
		Password: models.PASSWORD,
	}
	sesh, err := mgo.DialWithInfo(mgoDialInfo)
	defer sesh.Close()

	if err != nil {
		log.Println("Could not connect to DB")
		panic(err)
	}

	db := sesh.DB(models.DbName)

	populators.Populate(db)
}
