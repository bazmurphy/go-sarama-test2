package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("failed to start producer: %v", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	var producerEnqueuedCount, producerSuccessCount, producerErrorCount int

	go func() {
		log.Println("producer loop started...")
		defer log.Println("producer loop ended...")

		for {
			newMessageKey := uuid.NewString()
			newMessageValue := "testing 123"

			newMessage := &sarama.ProducerMessage{
				Topic: "my_topic",
				Key:   sarama.StringEncoder(newMessageKey),
				Value: sarama.StringEncoder(newMessageValue),
			}

			select {
			case producer.Input() <- newMessage:
				producerEnqueuedCount++
			case message := <-producer.Successes():
				log.Println("success producing message:")
				log.Println("\ttopic:", message.Topic)
				log.Println("\toffset:", message.Offset)
				log.Println("\tpartition:", message.Partition)
				log.Println("\tkey:", message.Key)
				log.Println("\tvalue:", message.Value)
				producerSuccessCount++
			case err := <-producer.Errors():
				log.Println("error producing message:", err)
				producerErrorCount++
			case <-signals:
				// AsyncClose triggers a shutdown of the producer.
				// The shutdown has completed when both the Errors and Successes channels have been closed.
				// When calling AsyncClose, you *must* continue to read from those channels in order to drain the results of any messages in flight.
				producer.AsyncClose()
				return
			}

			time.Sleep(1 * time.Second)
		}
	}()

	<-signals

	// Close shuts down the producer and waits for any buffered messages to be flushed.
	// You must call this function before a producer object passes out of scope, as it may otherwise leak memory.
	// You must call this before process shutting down, or you may lose messages.
	// You must call this before calling Close on the underlying client.
	producer.Close()

	log.Println("metrics:")
	log.Println("\tproducer enqueued count:", producerEnqueuedCount)
	log.Println("\tproducer success count:", producerSuccessCount)
	log.Println("\tproducer error count:", producerErrorCount)
}
