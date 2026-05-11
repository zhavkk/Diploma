package tlsconfig

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// generateTestCert creates a test certificate and key with the given validity period.
// Returns the certificate and key PEM encoded bytes.
func generateTestCert(commonName string, isCA bool, notBefore, notAfter time.Time) ([]byte, []byte, error) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"Test Organization"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}

	if isCA {
		template.KeyUsage |= x509.KeyUsageCertSign | x509.KeyUsageCRLSign
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privKey.PublicKey, privKey)
	if err != nil {
		return nil, nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})

	keyBytes, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		return nil, nil, err
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: keyBytes,
	})

	return certPEM, keyPEM, nil
}

func TestValidateCertificates_NoCertificates(t *testing.T) {
	result, err := ValidateCertificates("", "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Certificates)
	assert.Empty(t, result.Expired)
	assert.Empty(t, result.Expiring)
}

func TestValidateCertificates_ValidCert(t *testing.T) {
	now := time.Now()
	certPEM, keyPEM, err := generateTestCert("test.example.com", false, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))
	require.NoError(t, err)

	certFile := filepath.Join(t.TempDir(), "cert.pem")
	keyFile := filepath.Join(t.TempDir(), "key.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0600))

	result, err := ValidateCertificates(certFile, keyFile, "")
	require.NoError(t, err)
	assert.Len(t, result.Certificates, 1)
	assert.Empty(t, result.Expired)
	assert.Empty(t, result.Expiring)
	assert.Equal(t, "test.example.com", result.Certificates[0].Subject)
	assert.False(t, result.Certificates[0].IsCA)
}

func TestValidateCertificates_ExpiredCert(t *testing.T) {
	now := time.Now()
	certPEM, keyPEM, err := generateTestCert("expired.example.com", false, now.Add(-48*24*time.Hour), now.Add(-24*24*time.Hour))
	require.NoError(t, err)

	certFile := filepath.Join(t.TempDir(), "cert.pem")
	keyFile := filepath.Join(t.TempDir(), "key.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0600))

	result, err := ValidateCertificates(certFile, keyFile, "")
	require.NoError(t, err)
	assert.Len(t, result.Certificates, 1)
	assert.Len(t, result.Expired, 1)
	assert.Empty(t, result.Expiring)
	assert.Equal(t, "expired.example.com", result.Expired[0].Subject)
}

func TestValidateCertificates_ExpiringSoonCert(t *testing.T) {
	now := time.Now()
	// Expires in 15 days (within the 30-day threshold)
	certPEM, keyPEM, err := generateTestCert("expiring.example.com", false, now.Add(-15*24*time.Hour), now.Add(15*24*time.Hour))
	require.NoError(t, err)

	certFile := filepath.Join(t.TempDir(), "cert.pem")
	keyFile := filepath.Join(t.TempDir(), "key.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0600))

	result, err := ValidateCertificates(certFile, keyFile, "")
	require.NoError(t, err)
	assert.Len(t, result.Certificates, 1)
	assert.Empty(t, result.Expired)
	assert.Len(t, result.Expiring, 1)
	assert.Equal(t, "expiring.example.com", result.Expiring[0].Subject)
}

func TestValidateCertificates_JustAboveThreshold(t *testing.T) {
	now := time.Now()
	// Expires in 31 days (just above the 30-day threshold)
	certPEM, keyPEM, err := generateTestCert("valid.example.com", false, now.Add(-4*24*time.Hour), now.Add(31*24*time.Hour))
	require.NoError(t, err)

	certFile := filepath.Join(t.TempDir(), "cert.pem")
	keyFile := filepath.Join(t.TempDir(), "key.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0600))

	result, err := ValidateCertificates(certFile, keyFile, "")
	require.NoError(t, err)
	assert.Len(t, result.Certificates, 1)
	assert.Empty(t, result.Expired)
	assert.Empty(t, result.Expiring)
	assert.Equal(t, "valid.example.com", result.Certificates[0].Subject)
}

func TestValidateCertificates_CACert(t *testing.T) {
	now := time.Now()
	certPEM, _, err := generateTestCert("test-ca.example.com", true, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))
	require.NoError(t, err)

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, certPEM, 0600))

	result, err := ValidateCertificates("", "", caFile)
	require.NoError(t, err)
	assert.Len(t, result.Certificates, 1)
	assert.Empty(t, result.Expired)
	assert.Empty(t, result.Expiring)
	assert.Equal(t, "test-ca.example.com", result.Certificates[0].Subject)
	assert.True(t, result.Certificates[0].IsCA)
}

func TestValidateCertificates_ExpiredCACert(t *testing.T) {
	now := time.Now()
	certPEM, _, err := generateTestCert("expired-ca.example.com", true, now.Add(-48*24*time.Hour), now.Add(-24*24*time.Hour))
	require.NoError(t, err)

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, certPEM, 0600))

	result, err := ValidateCertificates("", "", caFile)
	require.NoError(t, err)
	assert.Len(t, result.Certificates, 1)
	assert.Len(t, result.Expired, 1)
	assert.Equal(t, "expired-ca.example.com", result.Expired[0].Subject)
	assert.True(t, result.Expired[0].IsCA)
}

func TestValidateCertificates_BothCertAndCA(t *testing.T) {
	now := time.Now()

	// Create CA
	caCertPEM, _, err := generateTestCert("test-ca.example.com", true, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))
	require.NoError(t, err)

	// Create server cert
	serverCertPEM, serverKeyPEM, err := generateTestCert("server.example.com", false, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))
	require.NoError(t, err)

	certFile := filepath.Join(t.TempDir(), "cert.pem")
	keyFile := filepath.Join(t.TempDir(), "key.pem")
	caFile := filepath.Join(t.TempDir(), "ca.pem")

	require.NoError(t, os.WriteFile(certFile, serverCertPEM, 0600))
	require.NoError(t, os.WriteFile(keyFile, serverKeyPEM, 0600))
	require.NoError(t, os.WriteFile(caFile, caCertPEM, 0600))

	result, err := ValidateCertificates(certFile, keyFile, caFile)
	require.NoError(t, err)
	assert.Len(t, result.Certificates, 2)
	assert.Empty(t, result.Expired)
	assert.Empty(t, result.Expiring)
}

func TestValidateCertificates_MissingCertFile(t *testing.T) {
	_, err := ValidateCertificates("/nonexistent/cert.pem", "", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read server certificate")
}

func TestValidateCertificates_MissingKeyFile(t *testing.T) {
	certFile := filepath.Join(t.TempDir(), "cert.pem")
	require.NoError(t, os.WriteFile(certFile, []byte("test"), 0600))

	_, err := ValidateCertificates(certFile, "/nonexistent/key.pem", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read server key")
}

func TestValidateCertificates_MissingCAFile(t *testing.T) {
	_, err := ValidateCertificates("", "", "/nonexistent/ca.pem")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read CA certificate")
}

func TestValidateCertificates_InvalidPEM(t *testing.T) {
	certFile := filepath.Join(t.TempDir(), "cert.pem")
	keyFile := filepath.Join(t.TempDir(), "key.pem")
	require.NoError(t, os.WriteFile(certFile, []byte("not a pem"), 0600))
	require.NoError(t, os.WriteFile(keyFile, []byte("not a pem"), 0600))

	_, err := ValidateCertificates(certFile, keyFile, "")
	assert.Error(t, err)
}

func TestLogValidationResults_NilResult(t *testing.T) {
	logger := zaptest.NewLogger(t)
	// Should not panic
	LogValidationResults(logger, nil)
}

func TestLogValidationResults_NoCertificates(t *testing.T) {
	logger := zaptest.NewLogger(t)
	result := &ValidationResult{
		Certificates: []CertInfo{},
	}
	// Should not panic
	LogValidationResults(logger, result)
}

func TestLogValidationResults_ValidCertificates(t *testing.T) {
	logger := zaptest.NewLogger(t)
	result := &ValidationResult{
		Certificates: []CertInfo{
			{
				Subject:   "test.example.com",
				Issuer:    "test-ca.example.com",
				NotBefore: time.Now().Add(-24 * time.Hour),
				NotAfter:  time.Now().Add(365 * 24 * time.Hour),
				IsCA:      false,
				DNSNames:  []string{"test.example.com"},
			},
		},
	}
	// Should not panic
	LogValidationResults(logger, result)
}

func TestCertInfoFromX509(t *testing.T) {
	now := time.Now()
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test.example.com"},
		Issuer:                pkix.Name{CommonName: "test-ca.example.com"},
		NotBefore:             now.Add(-24 * time.Hour),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		IsCA:                  false,
		DNSNames:              []string{"test.example.com", "alt.example.com"},
		IPAddresses:           nil,
		BasicConstraintsValid: true,
	}

	info := certInfoFromX509(&template)
	assert.Equal(t, "test.example.com", info.Subject)
	assert.Equal(t, "test-ca.example.com", info.Issuer)
	assert.Equal(t, now.Add(-24*time.Hour), info.NotBefore)
	assert.Equal(t, now.Add(365*24*time.Hour), info.NotAfter)
	assert.False(t, info.IsCA)
	assert.Equal(t, []string{"test.example.com", "alt.example.com"}, info.DNSNames)
}

func TestParseCertificatesFromPEM(t *testing.T) {
	now := time.Now()
	cert1PEM, _, err := generateTestCert("cert1.example.com", false, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))
	require.NoError(t, err)

	cert2PEM, _, err := generateTestCert("cert2.example.com", false, now.Add(-24*time.Hour), now.Add(365*24*time.Hour))
	require.NoError(t, err)

	// Concatenate PEM blocks
	combinedPEM := string(cert1PEM) + string(cert2PEM)

	certs, err := parseCertificatesFromPEM([]byte(combinedPEM))
	require.NoError(t, err)
	assert.Len(t, certs, 2)
}

func TestParseCertificatesFromPEM_InvalidPEM(t *testing.T) {
	_, err := parseCertificatesFromPEM([]byte("not a pem"))
	assert.Error(t, err)
}

func TestParseCertificatesFromPEM_NoCertificates(t *testing.T) {
	_, err := parseCertificatesFromPEM([]byte("-----BEGIN SOMETHING-----\n-----END SOMETHING-----"))
	assert.Error(t, err)
}
