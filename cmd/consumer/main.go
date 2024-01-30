package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	/* */

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "16-apache-kafka-kafka-1:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
	}

	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		fmt.Println("Error consumer", err.Error())
	}

	topics := []string{"teste2"}
	c.SubscribeTopics(topics, nil)

	for { //O consumer fica em loop esperando mensagens
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		}
	}
}
