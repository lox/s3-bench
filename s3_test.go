package s3bench

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	"github.com/dustin/go-humanize"

	"log"

	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)

var payloadKs = flag.Int64("bench.payload", 512, "kbytes to use as payload")
var payload []byte
var random *randomData
var ts time.Time

type randomData struct {
	src rand.Source
}

func (r *randomData) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = byte(r.src.Int63() & 0xff)
	}
	return len(p), nil
}

func init() {
	flag.Parse()

	random = &randomData{
		rand.NewSource(time.Now().UnixNano()),
	}

	buf := &bytes.Buffer{}
	io.CopyN(buf, random, *payloadKs*int64(1024))

	log.Printf("Generated %s of payload",
		humanize.Bytes(uint64(buf.Len())))

	payload = buf.Bytes()
	ts = time.Now()
}

// bucket creates a random bucket in the given region
func bucket(region aws.Region) *s3.Bucket {
	auth, err := aws.EnvAuth()
	if err != nil {
		panic(err)
	}

	conn := s3.New(auth, region)
	bucket := conn.Bucket(fmt.Sprintf("s3-bench-%s-%d",
		region.Name, ts.Nanosecond()))

	if err := bucket.PutBucket(s3.Private); err != nil {
		if err.(*s3.Error).StatusCode != 409 {
			panic(err)
		}
	}

	return bucket
}

func benchPut(payload []byte, b *testing.B, bucket *s3.Bucket) {
	ts := time.Now()

	for n := 0; n < b.N; n++ {
		err := bucket.PutReader(
			"random.dat", bytes.NewReader(payload),
			int64(len(payload)), "application/binary", s3.Private)
		if err != nil {
			b.Fatal(err)
		}
	}

	blen := int64(b.N * len(payload))
	dur := time.Now().Sub(ts)
	kSpeed := float64(blen) / dur.Seconds() / float64(1024)

	b.Logf("Wrote %s in %s (%.4fk/s)",
		humanize.Bytes(uint64(blen)), dur, kSpeed)
}

func benchGet(b *testing.B, bucket *s3.Bucket) {
	ts := time.Now()

	for n := 0; n < b.N; n++ {
		rc, err := bucket.GetReader("random.dat")
		if err != nil {
			b.Fatal(err)
		}
		ioutil.ReadAll(rc)
		rc.Close()
	}

	blen := b.N * len(payload)
	dur := time.Now().Sub(ts)
	kSpeed := float64(blen) / dur.Seconds() / float64(1024)

	b.Logf("Read %s in %s (%.4fk/s)",
		humanize.Bytes(uint64(blen)), dur, kSpeed)
}

func cleanup(bucket *s3.Bucket) {
	bucket.Del("random.dat")
	bucket.DelBucket()
}

func TestPayloadExists(t *testing.T) {
	if len(payload) == 0 {
		t.Fatal("Expected to have bytes to work with")
	}
}

func BenchmarkPutAPNortheast(b *testing.B) {
	benchPut(payload, b, bucket(aws.APNortheast))
}

func BenchmarkGetAPNortheast(b *testing.B) {
	bucket := bucket(aws.APNortheast)
	defer cleanup(bucket)
	benchGet(b, bucket)
}

func BenchmarkPutAPSoutheast(b *testing.B) {
	benchPut(payload, b, bucket(aws.APSoutheast))
}

func BenchmarkGetAPSoutheast(b *testing.B) {
	bucket := bucket(aws.APSoutheast)
	defer cleanup(bucket)
	benchGet(b, bucket)
}

func BenchmarkPutAPSoutheast2(b *testing.B) {
	benchPut(payload, b, bucket(aws.APSoutheast2))
}

func BenchmarkGetAPSoutheast2(b *testing.B) {
	bucket := bucket(aws.APSoutheast2)
	defer cleanup(bucket)
	benchGet(b, bucket)
}

// func BenchmarkPutCNNorth(b *testing.B) {
// 	// Needs AWS4-HMAC-SHA256
// 	// benchPut(payload, b, bucket(aws.CNNorth))
// }

func BenchmarkPutEUWest(b *testing.B) {
	benchPut(payload, b, bucket(aws.EUWest))
}

func BenchmarkGetEUWest(b *testing.B) {
	bucket := bucket(aws.EUWest)
	defer cleanup(bucket)
	benchGet(b, bucket)
}

func BenchmarkPutSAEast(b *testing.B) {
	benchPut(payload, b, bucket(aws.SAEast))
}

func BenchmarkGetSAEast(b *testing.B) {
	bucket := bucket(aws.SAEast)
	defer cleanup(bucket)
	benchGet(b, bucket)
}

func BenchmarkPutUSWest(b *testing.B) {
	benchPut(payload, b, bucket(aws.USWest))
}

func BenchmarkGetUSWest(b *testing.B) {
	bucket := bucket(aws.USWest)
	defer cleanup(bucket)
	benchGet(b, bucket)
}

func BenchmarkPutUSWest2(b *testing.B) {
	benchPut(payload, b, bucket(aws.USWest2))
}

func BenchmarkGetUSWest2(b *testing.B) {
	bucket := bucket(aws.USWest2)
	defer cleanup(bucket)
	benchGet(b, bucket)
}

func BenchmarkPutUSEast(b *testing.B) {
	benchPut(payload, b, bucket(aws.USEast))
}

func BenchmarkGetUSEast(b *testing.B) {
	bucket := bucket(aws.USEast)
	defer cleanup(bucket)
	benchGet(b, bucket)
}
