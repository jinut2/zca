package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	mongoLib "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"gopkg.in/mgo.v2/bson"
)

var dev = true
var mongoURI = "mongodb://localhost:27017"

func main() {
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

type Data struct {
	ID          primitive.ObjectID `bson:"_id"`
	Field1      int                `bson:"field_1"`
	Field2      int                `bson:"field_2"`
	Fielda      string             `bson:"field_a"`
	Fieldb      string             `bson:"field_b"`
	Timestamp   int64              `bson:"timestamp"`
	Count       int                `bson:"count"`
	LastUpdated int64              `bson:"last_updated"`
}

func getCustomTLSConfig(caFile string) (*tls.Config, error) {
	tlsConfig := new(tls.Config)
	certs, err := ioutil.ReadFile(caFile)

	if err != nil {
		return tlsConfig, err
	}

	tlsConfig.RootCAs = x509.NewCertPool()
	ok := tlsConfig.RootCAs.AppendCertsFromPEM(certs)

	if !ok {
		return tlsConfig, errors.New("Failed parsing pem file")
	}

	return tlsConfig, nil
}

func connect(ctx context.Context) *mongo.Client {
	var tlsConfig *tls.Config
	var err error
	if !dev {
		tlsConfig, err = getCustomTLSConfig("./rds-combined-ca-bundle.pem")
		if err != nil {
			log.Fatalf("Failed getting TLS configuration: %v", err)
		}
	}
	mongoReaderClientOptions := options.Client().ApplyURI(mongoURI).SetTLSConfig(tlsConfig)
	client, err := mongoLib.Connect(context.TODO(), mongoReaderClientOptions)
	if err != nil {
		log.Fatalln("Mongo Connection Failed:", err.Error())
	}
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatalln("Mongo Ping Failed:", err.Error())
	}
	log.Println("Mongo Connection Success.")
	return client
}

func insertData(client *mongo.Client, n int) {
	c := client.Database("db-t01").Collection("collection-t01")

	data := make([]interface{}, n)
	for i := 1; i <= n; i++ {
		item := Data{
			ID:        primitive.NewObjectID(),
			Field1:    i,
			Field2:    i - 1,
			Fielda:    strconv.Itoa(i),
			Fieldb:    strconv.Itoa(i - 1),
			Count:     i % 20,
			Timestamp: time.Now().Unix(),
		}
		data[i-1] = item
	}
	log.Println("Inserting...")
	start := time.Now()
	_, err := c.InsertMany(context.TODO(), data)
	if err != nil {
		log.Fatalln("Err. c.InsertMany,", err.Error())
	}
	log.Println(">>>> Inserted", n, "rows in", time.Since(start))
}

func setIndices(client *mongo.Client) {
	c := client.Database("db-t01").Collection("collection-t01")
	indices := []mongo.IndexModel{}

	uniqueIndex := mongo.IndexModel{}
	uniqueKeys := bsonx.Doc{
		{
			Key: "field_a", Value: bsonx.Int32(int32(-1)),
		},
		{
			Key: "field_b", Value: bsonx.Int32(int32(-1)),
		},
		{
			Key: "field_1", Value: bsonx.Int32(int32(1)),
		},
		{
			Key: "field_2", Value: bsonx.Int32(int32(1)),
		},
		{
			Key: "timestamp", Value: bsonx.Int32(int32(-1)),
		},
	}
	uniqueIndex.Keys = uniqueKeys
	uniqueIndex.Options = options.Index().SetUnique(true)
	indices = append(indices, uniqueIndex)

	timestampIndex := mongo.IndexModel{}
	keys := bsonx.Doc{
		{
			Key: "timestamp", Value: bsonx.Int32(int32(-1)),
		},
	}
	timestampIndex.Keys = keys
	indices = append(indices, timestampIndex)

	opts := options.CreateIndexes()
	name, err := c.Indexes().CreateMany(context.Background(), indices, opts)
	if err != nil {
		log.Fatalln("Err create Index:", err)
	}
	log.Println("Successfully created:", name)
}

func asyncUpdateData(ctx context.Context, wg *sync.WaitGroup, client *mongo.Client, ids []primitive.ObjectID, count int) {
	defer wg.Done()
	updateData(ctx, client, ids, count)
}

func updateData(ctx context.Context, client *mongo.Client, ids []primitive.ObjectID, count int) {
	c := client.Database("db-t01").Collection("collection-t01")
	log.Println("updating data...")
	wms := []mongo.WriteModel{}
	now := time.Now().Unix()
	update := bson.M{"$inc": bson.M{"count": count}, "$set": bson.M{"last_updated": now}}
	wms = append(wms, mongo.NewUpdateManyModel().SetFilter(bson.M{"_id": bson.M{"$in": ids}}).SetUpdate(update))
	start := time.Now()
	_, err := c.BulkWrite(ctx, wms)
	if err != nil {
		log.Fatalln("Bulk Write Err.", err.Error())
	}
	log.Println(count, ")  ", time.Since(start), "taken for updating", len(ids))
}

func getAllIDs(ctx context.Context, client *mongo.Client) []primitive.ObjectID {
	c := client.Database("db-t01").Collection("collection-t01")
	ids := []primitive.ObjectID{}
	results := []Data{}
	start := time.Now()
	cur, err := c.Find(ctx, bson.M{})
	if err != nil {
		log.Fatalln("Get Err.", err.Error())
	}
	err = cur.All(ctx, &results)
	if err != nil {
		log.Fatalln("Cur.All Err.", err.Error())
	}
	log.Println(time.Since(start), "taken for fetching", len(results), "documents.")
	for _, each := range results {
		ids = append(ids, each.ID)
	}
	return ids
}

func dropData(ctx context.Context, client *mongo.Client) {
	c := client.Database("db-t01").Collection("collection-t01")
	c.Drop(ctx)
	log.Println("Dropped collection.")
}
