package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"regexp"
	"runtime"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/lox/s3-bench/s3"
)

var (
	payloadSize int64
	fileName    string = "random.dat"
	testRuns    int
	filter      string
	cleanup     bool
)

func main() {
	flag.IntVar(&testRuns, "runs", 3, "the number of times to run each test")
	flag.Int64Var(&payloadSize, "payload", 5000, "kbytes to use as payload")
	flag.StringVar(&filter, "filter", ".", "a pattern to use to filter region names")
	flag.BoolVar(&cleanup, "cleanup", true, "whether to remove buckets after tests")
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	// payloadSize is initially in KB
	payloadSize = payloadSize * int64(1024)

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

	re := regexp.MustCompile(filter)

	for _, region := range s3.Regions {
		if re.MatchString(region.Name) {
			log.Printf("Testing region %s", region.Name)

			suffix := fmt.Sprintf("%d", time.Now().Nanosecond())
			bucket, err := bucket(suffix, region)
			if err != nil {
				log.Println(err)
				continue
			}

			if cleanup {
				defer func() {
					bucket.Del(fileName)
					bucket.Del()
				}()
			}

			if err := benchPut(payload.Bytes(), bucket); err != nil {
				log.Fatal(err)
			}

			if err := benchGet(bucket); err != nil {
				log.Fatal(err)
			}

			if err := benchMultiGet(bucket); err != nil {
				log.Fatal(err)
			}

			if cleanup {
				bucket.Del(fileName)
				bucket.Del()
			}

		}
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
func bucket(suffix string, reg s3.Region) (*s3.Bucket, error) {
	bucket := &s3.Bucket{
		Name:   fmt.Sprintf("s3-bench-%s-%s", reg.Name, suffix),
		Region: reg,
	}

	if err := bucket.Create(); err != nil {
		return nil, err
	}

	return bucket, nil
}

func benchPut(payload []byte, bucket *s3.Bucket) error {
	ts := time.Now()

	for i := 0; i < testRuns; i++ {
		if err := bucket.Put(fileName, payload, "public-read"); err != nil {
			return err
		}
		log.Printf("Wrote %s", bucket.URL(fileName))
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
		rc, err := bucket.Get(fileName)
		if err != nil {
			return err
		}
		io.Copy(ioutil.Discard, rc)
		rc.Close()
	}

	blen := payloadSize * int64(testRuns)
	dur := time.Now().Sub(ts)
	kSpeed := float64(blen) / dur.Seconds() / float64(1024)

	log.Printf("Read (in serial) %s in %s (%.4fk/s)",
		humanize.Bytes(uint64(blen)), dur, kSpeed)

	return nil
}

func benchMultiGet(bucket *s3.Bucket) error {
	ts := time.Now()

	for i := 0; i < testRuns; i++ {
		rc, err := bucket.MultiGet(fileName, 4)
		if err != nil {
			return err
		}
		io.Copy(ioutil.Discard, rc)
		rc.Close()
	}

	blen := payloadSize * int64(testRuns)
	dur := time.Now().Sub(ts)
	kSpeed := float64(blen) / dur.Seconds() / float64(1024)

	log.Printf("Read (in parallel) %s in %s (%.4fk/s)",
		humanize.Bytes(uint64(blen)), dur, kSpeed)

	return nil
}
