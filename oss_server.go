package main

import (
	"errors"
	"fmt"
	"github.com/alfredyang1986/blackmirror/bmalioss"
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmkafka"
	"github.com/elodina/go-avro"
	kafkaAvro "github.com/elodina/go-kafka-avro"
	"os"
)

const rawMetricsSchema = `{"namespace": "net.elodina.kafka.metrics","type": "record","name": "OssCmd","fields": [{"name": "id", "type": "string"},{"name": "bucketName",  "type": "string" },{"name": "objectKey",  "type": "string" },{"name": "objectValue",  "type": "bytes" }]}`

func main() {
	//本地测试
	os.Setenv("PKG_CONFIG_PATH", "$PKG_CONFIG_PATH:/usr/lib/pkgconfig/")
	os.Setenv("BM_KAFKA_CONF_HOME", "resource/kafkaconfig.json")
	os.Setenv("BM_OSS_CONF_HOME", "resource/ossconfig.json")

	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topics := []string{"oss-topic"}
	bkc.SubscribeTopics(topics, subscribeFunc)
}

func subscribeFunc(a interface{}) {

	bytes := a.([]byte)
	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	decoder := kafkaAvro.NewKafkaAvroDecoder(bkc.SchemaRepositoryUrl)
	decoded, err := decoder.Decode(bytes)
	if err != nil {
		panic(err.Error())
	}
	decodedRecord, ok := decoded.(*avro.GenericRecord)
	if ok {
		bucketName := decodedRecord.Get("bucketName").(string)
		objectKey := decodedRecord.Get("objectKey").(string)
		objectValue := decodedRecord.Get("objectValue").([]byte)
		fmt.Println("bucketName=", bucketName, " => objectKey=", objectKey)
		err = bmalioss.PutObject(bucketName, objectKey, objectValue)
		bmerror.PanicError(err)
	} else {
		panic(errors.New("subscribeFunc Error"))
	}

}
