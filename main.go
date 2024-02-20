package main

import (
	"context"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Output:
// Plan for Altering offsets to Topic customers.events
// -------_-------------------------------------------
// P1 Current 2 ---> 3
// P2 Current 656 ---> 100

type kafkaClient struct {
	admin *kafka.AdminClient
}

func NewKafkaClient(client *kafka.AdminClient) (*kafkaClient, error) {
	return &kafkaClient{
		admin: client,
	}, nil
}

func (k *kafkaClient) AlterGroupOffsets(group, topic string, offsets map[int]int) error {
	plannedPartitions := make([]kafka.TopicPartition, 0, 1)

	for partition, offset := range offsets {
		plan := kafka.TopicPartition{
			Topic:     &topic,
			Partition: int32(partition),
			Offset:    kafka.Offset(offset),
		}
		plannedPartitions = append(plannedPartitions, plan)
	}

	plannedGroupPartitions := []kafka.ConsumerGroupTopicPartitions{
		{
			Group:      group,
			Partitions: plannedPartitions,
		},
	}
	result, err := k.admin.AlterConsumerGroupOffsets(context.Background(), plannedGroupPartitions)
	if err != nil {
		return err
	}

	for _, v := range result.ConsumerGroupsTopicPartitions {
		for _, t := range v.Partitions {
			if t.Error != nil {
				return t.Error
			}
		}
	}

	return nil

}
func (k *kafkaClient) FetchGroupOffsets(group, topic string) (map[int]int, error) {
	currentOffsets := make(map[int]int)
	groupPartitions := []kafka.ConsumerGroupTopicPartitions{
		{
			Group:      group,
			Partitions: nil,
		},
	}
	result, err := k.admin.ListConsumerGroupOffsets(context.Background(), groupPartitions)
	if err != nil {
		return nil, err
	}

	for _, v := range result.ConsumerGroupsTopicPartitions {
		for _, t := range v.Partitions {
			if *t.Topic == topic {
				// TODO: Do we really need this integer conversion or can we change the type
				currentOffsets[int(t.Partition)] = int(t.Offset)
			}
		}
	}
	return currentOffsets, nil
}

func main() {

	args := os.Args
	if len(args) != 3 {
		writeError("error Command line. [Usage] reoffset topicname groupname")
		os.Exit(1)
	}
	topic := args[1]
	group := args[2]
	// Map of partition to plannedOffsets that we want to apply
	plannedOffsets := make(map[int]int)
	plannedOffsets[0] = 0
	plannedOffsets[1] = 6

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
		"socket.timeout.ms": "3000",
	})
	if err != nil {
		writeError("error while creating admin client: %v", err)
		os.Exit(1)
	}
	client, err := NewKafkaClient(adminClient)

	currentOffsets, err := client.FetchGroupOffsets(group, topic)
	if err != nil {
		writeError("error while fetching current group offsets: %v", err)
		os.Exit(1)
	}

	writePlan(topic, group, currentOffsets, plannedOffsets)
	err = client.AlterGroupOffsets(group, topic, plannedOffsets)
	if err != nil {
		writeError("error while altering group offsets: %v", err)
		os.Exit(1)
	}
}

func writeError(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
}

func writeOutput(format string, a ...interface{}) {
	fmt.Fprintf(os.Stdout, format, a...)
}

func writePlan(topic, group string, currentOffsets, plannedOffsets map[int]int) {

	planMessage := fmt.Sprintf("Plan for Altering offsets to Topic %s and Group %s\n", topic, group)
	writeOutput(planMessage)
	msgLength := len(planMessage)
	for i := 0; i < msgLength; i++ {
		writeOutput("-")
	}
	writeOutput("\n")
	for p, o := range plannedOffsets {
		writeOutput("P%d offset %d ----> %d\n", p, currentOffsets[p], o)
	}
}
