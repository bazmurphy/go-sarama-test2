package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("failed to start consumer: %v", err)
	}

	topic := "my_topic"
	var partition int32 = 0
	offset := sarama.OffsetNewest

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		log.Fatalf("failed to start partition consumer: %v", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	var consumerMessageCount, consumerErrorCount int

	go func() {
		log.Println("consumer loop started...")
		defer log.Println("consumer loop ended...")

		for {
			select {
			case message := <-partitionConsumer.Messages():
				log.Println("consumed message:")
				log.Println("\ttopic:", message.Topic)
				log.Println("\toffset:", message.Offset)
				log.Println("\tpartition:", message.Partition)
				log.Println("\tkey:", string(message.Key))
				log.Println("\tvalue:", string(message.Value))
				consumerMessageCount++
			case err := <-partitionConsumer.Errors():
				log.Println("error consuming message:", err)
				consumerErrorCount++
			case <-signals:
				// AsyncClose initiates a shutdown of the PartitionConsumer.
				// This method will return immediately, after which you should continue to service the 'Messages' and 'Errors' channels until they are empty.
				// It is required to call this function, or Close before a consumer object passes out of scope, as it will otherwise leak memory.
				// You must call this before calling Close on the underlying client.
				partitionConsumer.AsyncClose()
				return
			}
		}
	}()

	<-signals

	// Close shuts down the consumer.
	// It must be called after all child PartitionConsumers have already been closed.
	consumer.Close()

	log.Println("metrics:")
	log.Println("\tconsumer message count:", consumerMessageCount)
	log.Println("\tconsumer error count:", consumerErrorCount)
}
