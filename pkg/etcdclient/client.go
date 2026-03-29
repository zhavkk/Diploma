package etcdclient

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Config holds etcd connection parameters.
type Config struct {
	Endpoints   []string
	DialTimeout time.Duration
}

// Client is a thin wrapper around the etcd v3 client providing key-value and leader election operations.
type Client struct {
	etcd *clientv3.Client
}

// New creates a new etcd Client connected to the specified endpoints.
func New(cfg Config) (*Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("etcdclient: connect: %w", err)
	}
	return &Client{etcd: cli}, nil
}

// Put stores a key-value pair in etcd.
func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.etcd.Put(ctx, key, value)
	if err != nil {
		return fmt.Errorf("etcdclient: put %q: %w", key, err)
	}
	return nil
}

// Get retrieves the value for the given key from etcd, returning an empty string if not found.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	resp, err := c.etcd.Get(ctx, key)
	if err != nil {
		return "", fmt.Errorf("etcdclient: get %q: %w", key, err)
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

// Delete removes the given key from etcd.
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.etcd.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("etcdclient: delete %q: %w", key, err)
	}
	return nil
}

// Watch returns a channel that receives the new value each time the given key is updated in etcd.
func (c *Client) Watch(ctx context.Context, key string) <-chan string {
	ch := make(chan string, 8)
	watchCh := c.etcd.Watch(ctx, key)
	go func() {
		defer close(ch)
		for resp := range watchCh {
			for _, ev := range resp.Events {
				ch <- string(ev.Kv.Value)
			}
		}
	}()
	return ch
}

// ElectionHandle holds both the election and the underlying session so that
// Resign can close both, preventing a session leak.
type ElectionHandle struct {
	election *concurrency.Election
	session  *concurrency.Session
}

// SessionDone returns a channel that is closed when the underlying etcd session expires.
func (h *ElectionHandle) SessionDone() <-chan struct{} {
	return h.session.Done()
}

// Campaign participates in a leader election under the given name and blocks until elected.
func (c *Client) Campaign(ctx context.Context, electionName, candidateID string) (*ElectionHandle, error) {
	sess, err := concurrency.NewSession(c.etcd, concurrency.WithTTL(10))
	if err != nil {
		return nil, fmt.Errorf("etcdclient: new session: %w", err)
	}
	election := concurrency.NewElection(sess, electionName)
	if err := election.Campaign(ctx, candidateID); err != nil {
		_ = sess.Close()
		return nil, fmt.Errorf("etcdclient: campaign: %w", err)
	}
	return &ElectionHandle{election: election, session: sess}, nil
}

// Resign voluntarily gives up leadership and closes the associated session.
func (c *Client) Resign(ctx context.Context, handle *ElectionHandle) error {
	if err := handle.election.Resign(ctx); err != nil {
		return err
	}
	return handle.session.Close()
}

// Close closes the underlying etcd client connection.
func (c *Client) Close() error {
	return c.etcd.Close()
}
