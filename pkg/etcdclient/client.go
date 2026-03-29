package etcdclient

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Config struct {
	Endpoints   []string
	DialTimeout time.Duration
}

type Client struct {
	etcd *clientv3.Client
}

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

func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.etcd.Put(ctx, key, value)
	if err != nil {
		return fmt.Errorf("etcdclient: put %q: %w", key, err)
	}
	return nil
}

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

func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.etcd.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("etcdclient: delete %q: %w", key, err)
	}
	return nil
}

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

func (h *ElectionHandle) SessionDone() <-chan struct{} {
	return h.session.Done()
}

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

func (c *Client) Resign(ctx context.Context, handle *ElectionHandle) error {
	if err := handle.election.Resign(ctx); err != nil {
		return err
	}
	return handle.session.Close()
}

func (c *Client) Close() error {
	return c.etcd.Close()
}
