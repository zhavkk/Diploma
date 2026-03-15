package coordination

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/etcdclient"
)

const defaultEtcdDialTimeout = 5 * time.Second

type Config struct {
	EtcdEndpoints []string
	NodeID        string

	EtcdDialTimeout time.Duration
}

type EtcdBackend interface {
	Put(ctx context.Context, key, value string) error
	Get(ctx context.Context, key string) (string, error)
	Campaign(ctx context.Context, key, val string) (resign func(context.Context) error, err error)
}

type Module struct {
	cfg     Config
	backend EtcdBackend
	log     *zap.Logger
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
	m.log.Info("starting leader election campaign", zap.String("node_id", m.cfg.NodeID))
	resign, err := m.backend.Campaign(ctx, "/ha-orchestrator/leader", m.cfg.NodeID)
	if err != nil {
		m.log.Error("leader election failed", zap.Error(err))
		return
	}
	m.log.Info("became leader", zap.String("node_id", m.cfg.NodeID))
	<-ctx.Done()
	if resign != nil {
		_ = resign(context.Background())
	}
}

func (m *Module) IsLeader(ctx context.Context) (bool, error) {
	val, err := m.backend.Get(ctx, "/ha-orchestrator/leader")
	if err != nil {
		return false, err
	}
	return val == m.cfg.NodeID, nil
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

func (a *etcdBackendAdapter) Campaign(ctx context.Context, key, val string) (func(context.Context) error, error) {
	election, err := a.cli.Campaign(ctx, key, val)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		return a.cli.Resign(ctx, election)
	}, nil
}
