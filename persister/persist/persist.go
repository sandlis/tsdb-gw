package persist

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"

	"github.com/sirupsen/logrus"
)

// The persist package contains client to push metrics to a persister service
var (
	client  *Client
	enabled = false
)

type Config struct {
	Addr   string
	APIKey string
}

type Client struct {
	url    string
	client *http.Client
}

func NewClient(addr string) (*Client, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("unable to parse gw.addr: '%v'", addr)
	}

	return &Client{
		url:    u.String(),
		client: &http.Client{},
	}, nil
}

func (c *Client) PushIntake(payload []byte) error {
	c.client.Post(c.url, "application/json", bytes.NewBuffer(payload))
	return nil
}
func Init(addr string) {
	cli, err := NewClient(addr)

	if err != nil {
		logrus.Fatalf("unable to initialize peristor: %v", err)
	}

	client = cli
	enabled = true
}

func Persist(data []byte) error {
	if enabled {
		return client.PushIntake(data)
	}
	return nil
}
