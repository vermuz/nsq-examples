package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
)

var (
	showVersion      = flag.Bool("version", false, "print version string")
	topic            = flag.String("topic", "", "NSQ topic")
	channel          = flag.String("channel", "", "NSQ channel")
	nsqdTCPAddrs     = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type TailHandler struct{}

func (th TailHandler) HandleMessage(m *nsq.Message) error {
	_, err := os.Stdout.Write(m.Body)
	if err != nil {
		log.Fatalf("ERROR: failed to write to os.Stdout - %s", err)
	}
	_, err = os.Stdout.WriteString("\n")
	if err != nil {
		log.Fatalf("ERROR: failed to write to os.Stdout - %s", err)
	}
	return nil
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_tail v%s\n", util.BINARY_VERSION)
		return
	}

	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("tail%06d#ephemeral", rand.Int()%999999)
	}

	if *topic == "" {
		log.Fatal("--topic is required")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	cfg := nsq.NewConfig()
	cfg.UserAgent = fmt.Sprintf("nsq_tail/%s go-nsq/%s", util.BINARY_VERSION, nsq.VERSION)

	consumer, err := nsq.NewConsumer(*topic, *channel, cfg)
	if err != nil {
		log.Fatal(err)
	}

	consumer.AddHandler(&TailHandler{})

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-sigChan:
			consumer.Stop()
		}
	}
}
