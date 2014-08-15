package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"time"

	"github.com/dustin/go-humanize"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)

var (
	payloadSize int64
	fileName    string = "random.dat"
	testRuns    int
)

func main() {
	flag.IntVar(&testRuns, "runs", 3, "the number of times to run each test")
	flag.Int64Var(&payloadSize, "payload", 512, "kbytes to use as payload")
	flag.Parse()

	// payloadSize is initially in KB
	payloadSize = payloadSize * int64(1024)

	s3.RetryAttempts(true)

	payload := &bytes.Buffer{}
	payload.Grow(int(payloadSize))

	data := &randomData{
		rand.NewSource(time.Now().UnixNano()),
	}

	io.CopyN(payload, data, payloadSize)

	log.Printf("Generated %s of payload",
		humanize.Bytes(uint64(payload.Len())))

	log.Printf("Running %d iterations per region",
		testRuns)

	for _, region := range aws.Regions {
		log.Printf("Testing region %s", region.Name)

		suffix := fmt.Sprintf("%d", time.Now().Nanosecond())
		bucket := bucket(suffix, region)
		defer cleanup(bucket)

		benchPut(payload.Bytes(), bucket)
		benchGet(bucket)
	}
}

type randomData struct {
	src rand.Source
}

func (r *randomData) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = byte(r.src.Int63() & 0xff)
	}
	return len(p), nil
}

// bucket creates a random bucket in the given region
func bucket(suffix string, region aws.Region) *s3.Bucket {
	auth, err := aws.EnvAuth()
	if err != nil {
		panic(err)
	}

	conn := s3.New(auth, region)
	bucket := conn.Bucket(fmt.Sprintf("s3-bench-%s-%s",
		region.Name, suffix))

	if err := bucket.PutBucket(s3.Private); err != nil {
		if err.(*s3.Error).StatusCode != 409 {
			panic(err)
		}
	}

	return bucket
}

func benchPut(payload []byte, bucket *s3.Bucket) error {
	ts := time.Now()

	for i := 0; i < testRuns; i++ {
		err := bucket.PutReader(
			fileName, bytes.NewReader(payload),
			int64(len(payload)), "application/binary", s3.Private)
		if err != nil {
			return err
		}
	}

	blen := payloadSize * int64(testRuns)
	dur := time.Now().Sub(ts)
	kSpeed := float64(blen) / dur.Seconds() / float64(1024)

	log.Printf("Wrote %s in %s (%.4fk/s)",
		humanize.Bytes(uint64(blen)), dur, kSpeed)

	return nil
}

func benchGet(bucket *s3.Bucket) error {
	ts := time.Now()

	for i := 0; i < testRuns; i++ {
		rc, err := bucket.GetReader(fileName)
		if err != nil {
			return err
		}
		ioutil.ReadAll(rc)
		rc.Close()
	}

	blen := payloadSize * int64(testRuns)
	dur := time.Now().Sub(ts)
	kSpeed := float64(blen) / dur.Seconds() / float64(1024)

	log.Printf("Read %s in %s (%.4fk/s)",
		humanize.Bytes(uint64(blen)), dur, kSpeed)

	return nil
}

func cleanup(bucket *s3.Bucket) {
	bucket.Del(fileName)
	bucket.DelBucket()
}
