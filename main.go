package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	mongoLib "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"gopkg.in/mgo.v2/bson"
)

var app = &cli.App{
	Commands: []*cli.Command{
		{
			Name:  "setup",
			Usage: "sets the indices",
			Action: func(ctx *cli.Context) error {
				log.Println("Connecting Mongo.")
				client := connect(ctx.Context)
				defer client.Disconnect(ctx.Context)
				setIndices(client)
				return nil
			},
		},
		{
			Name:  "insert",
			Usage: "inserts data",
			Flags: []cli.Flag{
				&cli.IntFlag{
					Name:  "docs",
					Value: 1000000,
					Usage: "no. of documents to be inserted",
				},
			},
			Action: func(ctx *cli.Context) error {
				log.Println("Connecting Mongo.")
				client := connect(ctx.Context)
				defer client.Disconnect(ctx.Context)

				insertData(client, ctx.Int("docs"))
				return nil
			},
		},
		{
			Name:  "drop",
			Usage: "drop the collection",
			Action: func(ctx *cli.Context) error {
				log.Println("Connecting Mongo.")
				client := connect(ctx.Context)
				defer client.Disconnect(ctx.Context)
				dropData(ctx.Context, client)
				return nil
			},
		},
		{
			Name:  "update",
			Usage: "updates data",
			Flags: []cli.Flag{
				&cli.IntFlag{
					Name:    "no",
					Aliases: []string{"n"},
					Value:   4,
					Usage:   "no. of times to be executed",
				},
				&cli.IntFlag{
					Name:    "docs",
					Aliases: []string{"d"},
					Value:   4,
					Usage:   "no. of documents to be updated",
				},
			},
			Action: func(ctx *cli.Context) error {
				log.Println("Connecting Mongo.")
				client := connect(ctx.Context)
				defer client.Disconnect(ctx.Context)
				n := ctx.Int("no")
				d := ctx.Int("docs")
				ids := getAllIDs(ctx.Context, client)
				start := time.Now()
				for i := 0; i < n; i++ {
					updateData(ctx.Context, client, ids[0:d], i)
				}
				log.Println(">>>>", time.Since(start), "is the total time taken")
				return nil
			},
		},
		{
			Name:  "async-update",
			Usage: "updates data in different threads",
			Flags: []cli.Flag{
				&cli.IntFlag{
					Name:    "no",
					Aliases: []string{"n"},
					Value:   4,
					Usage:   "no. of threads to be executed",
				},
				&cli.IntFlag{
					Name:    "docs",
					Aliases: []string{"d"},
					Value:   4,
					Usage:   "no. of documents to be updated",
				},
			},
			Action: func(ctx *cli.Context) error {
				log.Println("Connecting Mongo.")
				client := connect(ctx.Context)
				defer client.Disconnect(ctx.Context)
				n := ctx.Int("no")
				d := ctx.Int("docs")
				ids := getAllIDs(ctx.Context, client)

				var wg sync.WaitGroup
				start := time.Now()
				for i := 1; i <= n; i++ {
					wg.Add(1)
					go asyncUpdateData(ctx.Context, &wg, client, ids[0:d], i)
				}
				wg.Wait()
				log.Println(">>>>", time.Since(start), "is the total time taken")
				return nil
			},
		},
	},
}

func main() {
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func run() {
	ctx := context.TODO()
	log.Println("Connecting Mongo.")
	client := connect(ctx)
	defer client.Disconnect(ctx)

	// setupDB(client)

	ids := getAllIDs(ctx, client)
	first := ids[0:100000]
	second := ids[0:100000]
	third := ids[0:100000]
	fourth := ids[0:100000]

	var wg sync.WaitGroup
	start := time.Now()
	wg.Add(1)
	go asyncUpdateData(ctx, &wg, client, first, 1)
	wg.Add(1)
	go asyncUpdateData(ctx, &wg, client, second, 2)
	wg.Add(1)
	go asyncUpdateData(ctx, &wg, client, third, 3)
	wg.Add(1)
	go asyncUpdateData(ctx, &wg, client, fourth, 4)
	wg.Wait()
	log.Println(">>>>", time.Since(start), "is the total time taken")
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

func connect(ctx context.Context) *mongo.Client {
	mongoReaderClientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
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
