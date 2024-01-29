package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {

	fmt.Println("Hello, Go!")
}

func NewKafkaProducer() *kafka.Producer {

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "16-apache-kafka-kafka-1:9092",
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}

	return p
}
