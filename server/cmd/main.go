package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"table-org/server/db"
	"table-org/server/utils"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var recordEntire []map[string]interface{}
var done chan *propagatedPanic

func goRoutine(record []map[string]interface{}, columnMap map[string]string, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()

		if v := recover(); v != nil {
			done <- &propagatedPanic{
				val:   v,
				stack: debug.Stack(),
			}

		} else {
			done <- nil
		}

	}()

	extractedData, err := utils.ExtractData(record, columnMap)
	if err != nil {
		fmt.Printf("error extracting data: %v", err)
		return
	}
	recordEntire = append(recordEntire, extractedData...)
}

type propagatedPanic struct {
	val   any
	stack []byte
}

func main() {

	//take input python st
	if len(os.Args) < 3 {
		fmt.Println("Please provide both Database Name and Collection Name.")
		return
	}

	database := os.Args[1]
	collection := os.Args[2]
	columnDictJSON := os.Args[3]

	//MANUAL

	// reader := bufio.NewReader(os.Stdin)
	// fmt.Print("Database: ")
	// database, _ := reader.ReadString('\n')
	// database = strings.TrimSpace(database)

	// fmt.Print("Collection: ")
	// collection, _ := reader.ReadString('\n')
	// collection = strings.TrimSpace(collection)

	start := time.Now()
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Printf("Error loading dotenv: %v", err)
	}

	//establishing mongo connection
	client, err := db.EstablishMongoConnection()
	if err != nil {
		return
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			fmt.Printf("Error closing mongo %v:", err)
		}
	}()

	//config mongo
	coll := client.Database(database).Collection(collection)
	countOpts := options.Count().SetHint("_id_")
	count, err := coll.CountDocuments(context.TODO(), bson.D{}, countOpts)
	mssg := fmt.Sprintf("Total records in %v.%v is: %d", database, collection, count)
	fmt.Println(mssg)

	//fetch from mongo
	filter := bson.D{}

	cursor, err := coll.Find(context.TODO(), filter)
	if err != nil {
		return
	}

	var records []map[string]interface{}

	fetch := time.Now()
	for cursor.Next(context.Background()) {
		var recBson bson.M
		if err := cursor.Decode(&recBson); err != nil {
			fmt.Printf("error decoding document: %v\n", err)
			continue
		}

		jsonData, err := json.Marshal(recBson)
		if err != nil {
			fmt.Printf("error marshaling into jsonData: %v", err)
		}

		var recordsEach map[string]interface{}
		err = json.Unmarshal(jsonData, &recordsEach)
		if err != nil {
			fmt.Printf("error unmarshaling into recordsEach: %v", err)
			continue

		}
		records = append(records, recordsEach)
	}

	fetchtime := time.Since(fetch)
	fmt.Printf("ingest took %v time\n", fetchtime)

	//reading columnMap file, unmarshing columndMap
	unmarsh := time.Now()
	columnFile := []byte(columnDictJSON)
	// columnFile, _ :=  
	fmt.Println(columnFile)
	var columnMap map[string]string
	if err != nil {
		fmt.Printf("Error while reading Column JSON: %v\n", err)
		return
	}
	err = json.Unmarshal(columnFile, &columnMap)
	if err != nil {
		fmt.Printf("Error unmarshaling ColumnMap: %v\n", err)
	}
	midway := time.Since(unmarsh)
	fmt.Printf("marshaling in json took: %v\n", midway)

	//Processing each record and formatting each

	//GO ROUTINE
	var wg sync.WaitGroup
	const batchSize = 1000
	totalRecords := len(records)

	for i := 0; i < totalRecords; i += batchSize {
		end := i + batchSize
		if end > totalRecords {
			end = totalRecords
		}
		batch := records[i:end]

		wg.Add(1)
		go func() {
			goRoutine(batch, columnMap, &wg)
			if val := <-done; val != nil {
				panic(val)
			}
		}()
	}

	numGoroutines := runtime.NumGoroutine()
	fmt.Printf("Number of Running Goroutines: %d\n", numGoroutines)

	wg.Wait()
	marshaledData, _ := json.MarshalIndent(recordEntire, "", "  ")
	err = os.WriteFile("FINAL.json", marshaledData, 0644)
	if err != nil {
		return
	}
	elapsed := time.Since(start)
	fmt.Printf("etl took %v\n", elapsed)

	defer os.Exit(0)

}
