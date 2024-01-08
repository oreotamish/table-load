package db

import (
	"context"
	"fmt"
	"os"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func EstablishMongoConnection() (*mongo.Client, error) {
	opts := options.Client().ApplyURI(os.Getenv("mongo_uri"))
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		fmt.Printf("Error connecting to mongo %v:", err)
		return nil, err
	}
	return client, nil

}
