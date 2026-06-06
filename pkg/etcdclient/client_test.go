package etcdclient_test

import (
	"context"
	"testing"
	"time"

	"github.com/zhavkk/Diploma/pkg/etcdclient"
)

func TestConfig_FieldsExist(t *testing.T) {
	_ = etcdclient.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}
}

func TestElectionHandle_TypeExists(t *testing.T) {

	type campaigner interface {
		Campaign(ctx interface{}, electionName, candidateID string) (*etcdclient.ElectionHandle, error)
	}

	_ = (*campaigner)(nil)
}

func TestNew_CreatesClientWithValidConfig(t *testing.T) {

	cli, err := etcdclient.New(etcdclient.Config{
		Endpoints:   []string{"127.0.0.1:2399"},
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	if cli == nil {
		t.Fatal("New() returned nil client")
	}

	if err := cli.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}
}

func TestPut_ReturnsErrorWithUnreachableEndpoint(t *testing.T) {

	cli, err := etcdclient.New(etcdclient.Config{
		Endpoints:   []string{"127.0.0.1:2399"},
		DialTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = cli.Put(ctx, "/test/key", "value")
	if err == nil {
		t.Error("expected Put to return an error with an unreachable endpoint")
	}
}

func TestClose_WorksWithoutError(t *testing.T) {
	cli, err := etcdclient.New(etcdclient.Config{
		Endpoints:   []string{"127.0.0.1:2399"},
		DialTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	err = cli.Close()
	if err != nil {
		t.Errorf("Close() returned unexpected error: %v", err)
	}
}
