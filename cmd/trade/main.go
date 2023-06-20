package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/lucassantin/FullCycle.git/internal/infra/kafka"
	"github.com/lucassantin/FullCycle.git/internal/market/dto"
	"github.com/lucassantin/FullCycle.git/internal/market/entity"
	"github.com/lucassantin/FullCycle.git/internal/market/transformer"
)

func main() {
	ordersInput := make(chan *entity.Order)
	ordersOutput := make(chan *entity.Order)
	wg := &sync.WaitGroup{}
	defer wg.await()

	kafkaMsgChan := make(chan *ckafka.Message)
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": "host.docker.internal:9094",
		"group.id":          "myGroup",
		"auto.offset.reset": "latest",
	}
	producer := kafka.NewKafkaProducer(configMap)
	kafka := kafka.NewConsumer(configMap, []string{"input"})

	go kafka.Consume(kafkaMsgChan)

	book := entity.NewBook(ordersIn, ordersOut, wg)
	go book.Trade()

	go func() {
		for msg := range kafkaMsgChan {
			wg.Add(1)
			fmt.Println(string(msg.Value))
			traderInput := dto.TradeInput{}
			err := json.Unmarshal(msg.Value, &traderInput)
			if err != nil {
				panic(err)
			}
			order := transformer.TransformInput(traderInput)
			ordersInput <- order
		}
	}()

	for res := range ordersOutput {
		output := transformer.TransformOutput(res)
		outputJson, err := json.MarshalIndent(output, "", "  ")
		fmt.Println(string(outputJson))
		if err != nil {
			fmt.Println(err)
		}
		producer.Publish(outuputJson, []byte("orders"), "output")
	}
}
