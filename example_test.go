package example

import (
	"fmt"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/localstack"
	"os"
	"testing"
	"time"
)

var (
	streamName         = "my-stream"
	streamShards int64 = 1
)

func TestKinesisIntegration(t *testing.T) {
	// set environment variables looked up by AWS: localstack doesn't perform any authentication, so any non-empty value will do
	_ = os.Setenv("AWS_ACCESS_KEY_ID", "x")
	_ = os.Setenv("AWS_SECRET_ACCESS_KEY", "y")

	container, err := gnomock.Start(
		localstack.Preset(localstack.WithServices(localstack.Kinesis)),
		gnomock.WithTimeout(time.Minute*2),
		gnomock.WithDebugMode(), // remove this if you don't want to see gnomock logs
	)
	if err != nil {
		t.Fatal(err.Error())
	}

	// this guarantees that we'll stop our container even when a panic occurs
	defer func() {
		if r := recover(); r != nil {
			_ = gnomock.Stop(container)
		}
	}()

	endpoint := fmt.Sprintf("http://%s/", container.Address(localstack.APIPort))
	kc, err := NewClient(&endpoint)
	if err != nil {
		t.Fatal(err.Error())
	}

	if err := kc.CreateStreamIfNotExists(&streamName, &streamShards); err != nil {
		t.Fatal(err.Error())
	}

	if err := kc.PutRecord(&streamName, "key", []byte("data")); err != nil {
		t.Fatal(err.Error())
	}

	records, err := kc.ConsumeRecords(&streamName)
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(records) != 1 {
		t.Fatalf("Expected 1 record, but got %v instead\n", len(records))
	}

	// stop our container at the end of the test
	_ = gnomock.Stop(container)
}
