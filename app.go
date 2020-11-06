package main

import (
	"log"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
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
