package example

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type KinesisClient struct {
	client *kinesis.Kinesis
}

func NewClient(endpoint *string) (*KinesisClient, error) {
	ses, err := session.NewSession(&aws.Config{
		Endpoint: endpoint,
		Region:      aws.String("eu-west-1"),
		DisableSSL:  aws.Bool(true),
		Credentials: credentials.NewEnvCredentials(),
	})

	if err != nil {
		return nil, err
	}

	return &KinesisClient{kinesis.New(ses)}, nil
}

func (kc *KinesisClient) CreateStreamIfNotExists(name *string, shardCount *int64) error {
	_, err := kc.client.CreateStream(&kinesis.CreateStreamInput{
		StreamName: name,
		ShardCount: shardCount,
	})
	if err != nil {
		if err := awsError(err); err != nil {
			return err
		}
	}

	err = kc.client.WaitUntilStreamExists(&kinesis.DescribeStreamInput{
		StreamName: name,
		Limit:      shardCount,
	})
	if err != nil {
		if err := awsError(err); err != nil {
			return err
		}
	}

	return nil
}

func (kc *KinesisClient) PutRecord(stream *string, key string, data []byte) error {
	_, err := kc.client.PutRecord(
		&kinesis.PutRecordInput{
			StreamName:   stream,
			PartitionKey: &key,
			Data:         data,
		})
	if err != nil {
		if err := awsError(err); err != nil {
			return err
		}
	}

	return nil
}

func (kc *KinesisClient) ConsumeRecords(streamName *string) ([]*kinesis.Record, error) {
	iterator, err := kc.getShardIterator(streamName)
	if err != nil {
		if err := awsError(err); err != nil {
			return nil, err
		}
	}

	output, err := kc.client.GetRecords(
		&kinesis.GetRecordsInput{
			Limit:         aws.Int64(1),
			ShardIterator: iterator,
		})
	if err != nil {
		if err := awsError(err); err != nil {
			return nil, err
		}
	}

	return output.Records, nil
}

func (kc *KinesisClient) getShardIterator(streamName *string) (*string, error) {
	shards, err := kc.client.ListShards(&kinesis.ListShardsInput{StreamName: streamName})
	if err != nil {
		if err := awsError(err); err != nil {
			return nil, err
		}
	}

	iteratorOutput, err := kc.client.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:        streamName,
		ShardId:           shards.Shards[0].ShardId, // don't do this at home! I'm relying on the fact that there's only one shard here
		ShardIteratorType: aws.String(kinesis.ShardIteratorTypeTrimHorizon),
	})
	if err != nil {
		if err := awsError(err); err != nil {
			return nil, err
		}
	}

	return iteratorOutput.ShardIterator, nil
}

func awsError(err error) error {
	if awsErr, ok := err.(awserr.Error); ok {
		var riue *kinesis.ResourceInUseException
		if !errors.As(awsErr, &riue) {
			return fmt.Errorf("AWS error encountered: %w", awsErr)
		}
	}

	return nil
}
