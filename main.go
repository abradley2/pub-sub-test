package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"cloud.google.com/go/pubsub"
)

type Config struct {
	Topic     string
	ProjectID string
}

func run(ctx context.Context, logger *log.Logger, cfg Config, topic *pubsub.Topic, done *chan error) {
	// add a reasonable delay so I dont bankrupt myself
	time.Sleep(1 * time.Second)

	msg := pubsub.Message{}

	base64DanAykroyd, err := getDanAykroydBase64()

	if err != nil {
		err = fmt.Errorf("getDanAykroydBase64 failed: %w", err)
		*done <- err
		return
	}

	msg.Data = base64DanAykroyd

	result := topic.Publish(ctx, &msg)

	pubID, err := result.Get(ctx)

	logger.Printf("Published message: %s", pubID)

	*done <- err
}

func main() {
	var logger *log.Logger
	var err error
	var done = make(chan error)
	var ctx = context.Background()

	logger = log.Default()

	config, err := readEnv()

	if err != nil {
		err = fmt.Errorf("readEnv failed: %w", err)
		logger.Fatal(err)
	}

	client, err := pubsub.NewClient(ctx, config.ProjectID)

	if err != nil {
		err = fmt.Errorf("pubsub.NewClient failed: %w", err)
		logger.Fatal(err)
	}

	topic := client.Topic(config.Topic)

	for err == nil {
		go run(ctx, logger, config, topic, &done)
		err = <-done
	}

	logger.Fatal(err)
}

func getDanAykroydBase64() ([]byte, error) {
	var err error
	danAykroydBase64 := make([]byte, 1000000)

	pwd, err := os.Getwd()
	if err != nil {
		return danAykroydBase64, fmt.Errorf("os.Getwd failed: %w", err)
	}

	p := path.Join(pwd, "images", "dan_aykroyd.jpg")

	danAykroyd, err := os.ReadFile(p)
	base64.RawStdEncoding.Encode(danAykroydBase64, danAykroyd)

	return danAykroydBase64, err
}

func readEnv() (Config, error) {
	var config Config
	var err error
	config.ProjectID = os.Getenv("PROJECT_ID")
	config.Topic = os.Getenv("TOPIC")

	if config.Topic == "" {
		err = fmt.Errorf("TOPIC is empty")
		return config, err
	}

	if config.ProjectID == "" {
		err = fmt.Errorf("PROJECT is empty")
		return config, err
	}

	return config, err
}
