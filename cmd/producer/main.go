package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {

	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()

	Publish("mensagem", "teste2", producer, nil, deliveryChan)

	go DeliveryReport(deliveryChan) //async

	/* Abaixo, estamos pegando o resultado do envio da mensagem. */
	//e := <-deliveryChan //Enquanto não vier uma mensagem para o delivery channel, o programa ficará pausado. Isso não é uma boa prática pois torna a comunicação síncrona.
	//msg := e.(*kafka.Message)
	//
	//if msg.TopicPartition.Error != nil {
	//	fmt.Println("Erro ao enviar mensagem")
	//} else {
	//	fmt.Println("Mensagem enviada:", msg.TopicPartition)
	//}
	//producer.Flush(1000)

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

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {

	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(message, nil)
	if err != nil {
		return err
	}

	return nil
}

/* Todas as vezes que uma nova mensagem chegar, o loop abaixo será executado. */
func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar mensagem")
			} else {
				fmt.Println("Mensagem enviada:", ev.TopicPartition)
				//Podemos anotar no banco de dados que a mensagem foi processada
				//Por exemplo, confirmando que uma transferência bancária ocorreu.

				//Se algum problema aconteceu, podemos tentar realizar o "retry".
			}
		}
	}
}
