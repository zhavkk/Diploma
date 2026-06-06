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

func ServerOption(certFile, keyFile string) (grpc.ServerOption, error) {
	creds, err := ServerCredentials(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return grpc.Creds(creds), nil
}

func ClientDialOption(caFile string) (grpc.DialOption, error) {
	creds, err := ClientCredentials(caFile)
	if err != nil {
		return nil, err
	}
	return grpc.WithTransportCredentials(creds), nil
}
