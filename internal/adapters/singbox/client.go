package singbox

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

type Options struct {
	APIURL     string
	ConfigPath string
	Token      string
	Timeout    time.Duration
	Logger     *slog.Logger
}

type Client struct {
	apiURL     string
	configPath string
	token      string
	http       *http.Client
	log        *slog.Logger
}

func New(opts Options) (*Client, error) {
	if opts.APIURL == "" {
		return nil, errors.New("singbox: APIURL required")
	}
	if opts.ConfigPath == "" {
		return nil, errors.New("singbox: ConfigPath required")
	}
	base := strings.TrimRight(opts.APIURL, "/")
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		return nil, errors.New("singbox: APIURL must include scheme")
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 15 * time.Second,
		}).DialContext,
		MaxIdleConns:          4,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   2 * time.Second,
		ResponseHeaderTimeout: 3 * time.Second,
		ExpectContinueTimeout: 500 * time.Millisecond,
	}
	return &Client{
		apiURL:     base,
		configPath: opts.ConfigPath,
		token:      opts.Token,
		http:       &http.Client{Transport: transport, Timeout: timeout},
		log:        log.With("component", "singbox"),
	}, nil
}

func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

func (c *Client) url(path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return c.apiURL + path
}

func (c *Client) authorize(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
}

func (c *Client) Name() string { return "singbox" }
