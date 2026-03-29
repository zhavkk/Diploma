package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ServerCredentials loads TLS server credentials from certFile and keyFile.
// If either path is empty, returns insecure credentials.
func ServerCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	if certFile == "" || keyFile == "" {
		return insecure.NewCredentials(), nil
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("tlsconfig: load key pair: %w", err)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}), nil
}

// ClientCredentials loads TLS client credentials using caFile for server verification.
// If caFile is empty, returns insecure credentials.
func ClientCredentials(caFile string) (credentials.TransportCredentials, error) {
	if caFile == "" {
		return insecure.NewCredentials(), nil
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tlsconfig: read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("tlsconfig: invalid CA certificate")
	}
	return credentials.NewClientTLSFromCert(pool, ""), nil
}

// ServerOption returns a grpc.ServerOption with the appropriate transport
// credentials. When certFile or keyFile is empty the server runs without TLS.
func ServerOption(certFile, keyFile string) (grpc.ServerOption, error) {
	creds, err := ServerCredentials(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return grpc.Creds(creds), nil
}

// ClientDialOption returns a grpc.DialOption with the appropriate transport
// credentials. When caFile is empty the client connects without TLS.
func ClientDialOption(caFile string) (grpc.DialOption, error) {
	creds, err := ClientCredentials(caFile)
	if err != nil {
		return nil, err
	}
	return grpc.WithTransportCredentials(creds), nil
}
