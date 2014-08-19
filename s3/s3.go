package s3

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"

	"github.com/htcat/htcat"
	"github.com/smartystreets/go-aws-auth"
)

var client = http.DefaultClient

var Regions = []Region{
	Region{"us-east-1", "s3.amazonaws.com", ""},
	Region{"us-west-2", "s3-us-west-2.amazonaws.com", "us-west-2"},
	Region{"us-west-1", "s3-us-west-1.amazonaws.com", "us-west-1"},
	Region{"eu-west-1", "s3-eu-west-1.amazonaws.com", "eu-west-1"},
	Region{"ap-southeast-1", "s3-ap-southeast-1.amazonaws.com", "ap-southeast-1"},
	Region{"ap-southeast-2", "s3-ap-southeast-2.amazonaws.com", "ap-southeast-2"},
	Region{"ap-northeast-1", "s3-ap-northeast-1.amazonaws.com", "ap-northeast-1"},
	Region{"sa-east-1", "s3-sa-east-1.amazonaws.com", "sa-east-1"},
}

type Region struct {
	Name               string
	Endpoint           string
	LocationConstraint string
}

type Bucket struct {
	Name   string
	Region Region
}

func (b *Bucket) Put(fn string, body []byte, acl string) error {
	h := http.Header{}
	h.Set("x-amz-acl", acl)

	_, err := b.Do("PUT", "/"+fn, h, body)
	if err != nil {
		return err
	}

	return nil
}

func (b *Bucket) Get(fn string) (io.ReadCloser, error) {
	resp, err := b.Do("GET", "/"+fn, http.Header{}, []byte{})
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (b *Bucket) MultiGet(fn string, concurrency int) (io.ReadCloser, error) {
	u, err := url.Parse(b.URL(fn))
	if err != nil {
		return nil, err
	}

	trans := &SignedTransport{http.DefaultTransport}
	htc := htcat.New(&http.Client{Transport: trans}, u, concurrency)

	r, wr := io.Pipe()
	go func() {
		htc.WriteTo(wr)
		r.Close()
	}()

	return r, nil
}

func (b *Bucket) Create() error {
	body := ""

	if b.Region.LocationConstraint != "" {
		body = `<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <LocationConstraint>` + b.Region.LocationConstraint + `</LocationConstraint>
        </CreateBucketConfiguration>`
	}

	resp, err := b.Do("PUT", "/", http.Header{}, []byte(body))
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	return err
}

func (b *Bucket) Del(paths ...string) error {
	for _, path := range paths {
		if resp, err := b.Do("DELETE", path, http.Header{}, []byte{}); err != nil {
			return err
		} else {
			defer resp.Body.Close()
		}
	}
	return nil
}

func (b *Bucket) URL(paths ...string) string {
	var path string = "/"

	if len(paths) > 0 {
		for _, p := range paths {
			path = path + strings.TrimPrefix(p, "/")
		}
	}

	return fmt.Sprintf("https://%s.%s%s", b.Name, b.Region.Endpoint, path)
}

func (b *Bucket) Do(method, path string, h http.Header, body []byte) (*http.Response, error) {
	req, err := http.NewRequest(method, b.URL(path), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	for k, headers := range h {
		for _, header := range headers {
			req.Header.Add(k, header)
		}
	}

	req.Header.Set("Connection", "Keep-Alive")
	awsauth.Sign(req)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if os.Getenv("DEBUG") != "" {
		log.Printf("%s %s => %d", req.Method, req.URL.String(), resp.StatusCode)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if os.Getenv("DEBUG") != "" {
			respb, err := httputil.DumpResponse(resp, true)
			if err != nil {
				return nil, err
			}
			log.Printf("%s", respb)
		}

		return resp, fmt.Errorf("Request failed with code %d", resp.StatusCode)
	}

	return resp, nil
}

type SignedTransport struct {
	http.RoundTripper
}

func (t *SignedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	awsauth.Sign(req)
	return t.RoundTripper.RoundTrip(req)
}
