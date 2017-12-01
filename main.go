package main

import (
	"github.com/adamhei/historicaldata/populators"
	"gopkg.in/mgo.v2"
)

const dbUrl = "localhost:27017"
const dbName = "historicaldata"

func main() {
	sesh, err := mgo.Dial(dbUrl)

	if err != nil {
		panic(err)
	}
	defer sesh.Close()

	populators.Populate(sesh.DB(dbName))
}
