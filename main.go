package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	uploadstatepb "github.com/file-uploaderr/file-verification-initiator/protogen"
	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func maybeDie(err error, msg string, args ...any) {
	if err != nil {
		die(msg, args...)
	}
}

func main() {
	err := godotenv.Load()
	maybeDie(err, "No .env found, when using environment variables:%v", err)
	brokerURL := os.Getenv("BROKER_URL")
	valueSubject := os.Getenv("SUBJECTVALUE")
	topic := os.Getenv("TOPIC")
	consumerGroup := os.Getenv("CONSUMER_GROUP")
	schemaRegistryURL := os.Getenv("REGISTRY_URL")

	fmt.Println(brokerURL, topic, consumerGroup, schemaRegistryURL)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	schemaRClient, err := sr.NewClient(sr.URLs(schemaRegistryURL))
	maybeDie(err, "unable to create schema registry client: %v", err)

	ss, err := schemaRClient.SchemaByVersion(ctx, valueSubject, 2)
	maybeDie(err, "unable to fetch protobuf schema: %v", err)
	fmt.Printf("using schema subject %q version %d id %d\n", ss.Subject, ss.Version, ss.ID)

	var serde sr.Serde
	serde.Register(
		ss.ID,
		&uploadstatepb.Envelope{},
		sr.Index(0),
		sr.DecodeFn(unMarshalProtobuf),
	)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokerURL, ",")...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topic),
		// kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)),
	)
	maybeDie(err, "unable to init kgo client: %v", err)
	defer cl.Close()

	for {
		fetches := cl.PollFetches(ctx)
		if errors.Is(fetches.Err0(), context.Canceled) || ctx.Err() != nil {
			fmt.Println("shutting down")
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			fmt.Fprintf(os.Stderr, "fetch error topic %s partition %d: %v\n", t, p, err)
		})

		fetches.EachRecord(func(r *kgo.Record) {
			msg := new(uploadstatepb.Envelope)
			if err := decode(ctx, schemaRClient, &serde, r.Value, msg); err != nil {
				fmt.Fprintf(os.Stderr, "unable to decode record at %s[%d]@%d: %v\n",
					r.Topic, r.Partition, r.Offset, err)
				return
			}
			fmt.Printf("Consumed %s[%d]@%d: %v\n", r.Topic, r.Partition, r.Offset, msg)
		})
	}
}

var unMarshalProtobuf = func(b []byte, v any) error { return proto.Unmarshal(b, v.(*uploadstatepb.Envelope)) }

func decode(ctx context.Context, reg *sr.Client, serde *sr.Serde, rawMessage []byte, targetMessageType *uploadstatepb.Envelope) error {
	err := serde.Decode(rawMessage, targetMessageType)
	if !errors.Is(err, sr.ErrNotRegistered) {
		return err
	}

	var h sr.ConfluentHeader
	id, _, err := h.DecodeID(rawMessage)
	if err != nil {
		return fmt.Errorf("bad header: %w", err)
	}

	s, err := reg.SchemaByID(ctx, id)
	if err != nil {
		return fmt.Errorf("schema id %d not in registry: %w", id, err)
	}
	fmt.Printf("new schema id %d appeared:\n%s\n", id, s.Schema)

	serde.Register(
		id,
		&uploadstatepb.Envelope{},
		sr.DecodeFn(unMarshalProtobuf),
		sr.Index(0),
	)

	return serde.Decode(rawMessage, targetMessageType)
}
