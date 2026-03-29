package coordination

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/etcdclient"
)

const defaultEtcdDialTimeout = 5 * time.Second

type Config struct {
	EtcdEndpoints []string
	NodeID        string

	EtcdDialTimeout time.Duration
	BackoffMin      time.Duration // defaults to 5s; set low in tests
}

type EtcdBackend interface {
	Put(ctx context.Context, key, value string) error
	Get(ctx context.Context, key string) (string, error)
	Campaign(ctx context.Context, key, val string) (resign func(context.Context) error, sessionDone <-chan struct{}, err error)
}

type Module struct {
	cfg          Config
	backend      EtcdBackend
	log          *zap.Logger
	leaderStatus atomic.Bool
}

func NewModule(cfg Config, log *zap.Logger) (*Module, error) {
	dialTimeout := cfg.EtcdDialTimeout
	if dialTimeout == 0 {
		dialTimeout = defaultEtcdDialTimeout
	}
	cli, err := etcdclient.New(etcdclient.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("coordination: etcd init: %w", err)
	}
	return &Module{cfg: cfg, backend: &etcdBackendAdapter{cli}, log: log}, nil
}

func NewModuleWithBackend(cfg Config, backend EtcdBackend, log *zap.Logger) *Module {
	return &Module{cfg: cfg, backend: backend, log: log}
}

func (m *Module) Run(ctx context.Context) {
	minBackoff := m.cfg.BackoffMin
	if minBackoff == 0 {
		minBackoff = 5 * time.Second
	}
	const maxBackoff = 30 * time.Second
	backoff := minBackoff

	for {
		m.log.Info("starting leader election campaign", zap.String("node_id", m.cfg.NodeID))
		resign, sessionDone, err := m.backend.Campaign(ctx, "/ha-orchestrator/leader", m.cfg.NodeID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.log.Error("leader election failed, retrying",
				zap.Error(err),
				zap.Duration("backoff", backoff),
			)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		backoff = minBackoff
		m.leaderStatus.Store(true)
		m.log.Info("became leader", zap.String("node_id", m.cfg.NodeID))

		select {
		case <-ctx.Done():
			if resign != nil {
				_ = resign(context.Background())
			}
			return
		case <-sessionDone:
			m.leaderStatus.Store(false)
			m.log.Warn("etcd session expired, re-campaigning", zap.String("node_id", m.cfg.NodeID))
			if resign != nil {
				_ = resign(context.Background())
			}
			// loop back to re-campaign
		}
	}
}

func (m *Module) IsLeader(_ context.Context) (bool, error) {
	return m.leaderStatus.Load(), nil
}

func (m *Module) PutClusterState(ctx context.Context, key, value string) error {
	return m.backend.Put(ctx, "/ha-orchestrator/state/"+key, value)
}

func (m *Module) GetClusterState(ctx context.Context, key string) (string, error) {
	return m.backend.Get(ctx, "/ha-orchestrator/state/"+key)
}

func (m *Module) Close() {
	if c, ok := m.backend.(*etcdBackendAdapter); ok {
		_ = c.cli.Close()
	}
}

type etcdBackendAdapter struct {
	cli *etcdclient.Client
}

func (a *etcdBackendAdapter) Put(ctx context.Context, key, value string) error {
	return a.cli.Put(ctx, key, value)
}

func (a *etcdBackendAdapter) Get(ctx context.Context, key string) (string, error) {
	return a.cli.Get(ctx, key)
}

func (a *etcdBackendAdapter) Campaign(ctx context.Context, key, val string) (func(context.Context) error, <-chan struct{}, error) {
	handle, err := a.cli.Campaign(ctx, key, val)
	if err != nil {
		return nil, nil, err
	}
	resignFn := func(ctx context.Context) error {
		return a.cli.Resign(ctx, handle)
	}
	return resignFn, handle.SessionDone(), nil
}
