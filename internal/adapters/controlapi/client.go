package controlapi

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"
)

type Options struct {
	BaseURL      string
	UserAgent    string
	Timeout      time.Duration
	TLSConfig    *tls.Config
	MaxIdleConns int
}

type Client struct {
	baseURL   string
	userAgent string
	http      *http.Client
}

func New(opts Options) (*Client, error) {
	if opts.BaseURL == "" {
		return nil, errors.New("controlapi: BaseURL required")
	}
	base := strings.TrimRight(opts.BaseURL, "/")
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		return nil, errors.New("controlapi: BaseURL must include scheme")
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	maxIdle := opts.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = 8
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxIdle,
		MaxIdleConnsPerHost:   maxIdle,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
		TLSClientConfig:       opts.TLSConfig,
	}

	ua := opts.UserAgent
	if ua == "" {
		ua = "go-node-agent"
	}

	return &Client{
		baseURL:   base,
		userAgent: ua,
		http: &http.Client{
			Transport: transport,
			Timeout:   timeout,
		},
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
	return c.baseURL + path
}
