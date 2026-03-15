package coordination

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/etcdclient"
)

type Config struct {
	EtcdEndpoints []string
	NodeID        string
}

type Module struct {
	cfg    Config
	client *etcdclient.Client
	log    *zap.Logger
}

func NewModule(cfg Config, log *zap.Logger) (*Module, error) {
	cli, err := etcdclient.New(etcdclient.Config{
		Endpoints: cfg.EtcdEndpoints,
	})
	if err != nil {
		return nil, fmt.Errorf("coordination: etcd init: %w", err)
	}
	return &Module{cfg: cfg, client: cli, log: log}, nil
}

func (m *Module) Run(ctx context.Context) {
	m.log.Info("starting leader election campaign", zap.String("node_id", m.cfg.NodeID))
	election, err := m.client.Campaign(ctx, "/ha-orchestrator/leader", m.cfg.NodeID)
	if err != nil {
		m.log.Error("leader election failed", zap.Error(err))
		return
	}
	m.log.Info("became leader", zap.String("node_id", m.cfg.NodeID))
	<-ctx.Done()
	_ = m.client.Resign(ctx, election)
}

func (m *Module) IsLeader(ctx context.Context) (bool, error) {
	val, err := m.client.Get(ctx, "/ha-orchestrator/leader")
	if err != nil {
		return false, err
	}
	return val == m.cfg.NodeID, nil
}

func (m *Module) PutClusterState(ctx context.Context, key, value string) error {
	return m.client.Put(ctx, "/ha-orchestrator/state/"+key, value)
}

func (m *Module) GetClusterState(ctx context.Context, key string) (string, error) {
	return m.client.Get(ctx, "/ha-orchestrator/state/"+key)
}

func (m *Module) Close() {
	_ = m.client.Close()
}
