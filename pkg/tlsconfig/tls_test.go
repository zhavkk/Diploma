package tlsconfig

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/credentials/insecure"
)

func TestServerCredentials_EmptyPaths(t *testing.T) {
	creds, err := ServerCredentials("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should return insecure credentials.
	want := insecure.NewCredentials().Info().SecurityProtocol
	if creds.Info().SecurityProtocol != want {
		t.Errorf("SecurityProtocol = %q, want %q", creds.Info().SecurityProtocol, want)
	}
}

func TestServerCredentials_MissingFiles(t *testing.T) {
	_, err := ServerCredentials("/nonexistent/cert.pem", "/nonexistent/key.pem")
	if err == nil {
		t.Error("expected error for missing cert files")
	}
}

func TestClientCredentials_EmptyPath(t *testing.T) {
	creds, err := ClientCredentials("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := insecure.NewCredentials().Info().SecurityProtocol
	if creds.Info().SecurityProtocol != want {
		t.Errorf("SecurityProtocol = %q, want %q", creds.Info().SecurityProtocol, want)
	}
}

func TestClientCredentials_MissingFile(t *testing.T) {
	_, err := ClientCredentials("/nonexistent/ca.pem")
	if err == nil {
		t.Error("expected error for missing CA file")
	}
}

func TestClientCredentials_InvalidPEM(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "bad-ca.pem")
	if err := os.WriteFile(caPath, []byte("not a certificate"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := ClientCredentials(caPath)
	if err == nil {
		t.Error("expected error for invalid PEM data")
	}
}

func TestServerOption_EmptyPaths(t *testing.T) {
	opt, err := ServerOption("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opt == nil {
		t.Error("expected non-nil server option")
	}
}

func TestClientDialOption_EmptyPath(t *testing.T) {
	opt, err := ClientDialOption("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opt == nil {
		t.Error("expected non-nil dial option")
	}
}
