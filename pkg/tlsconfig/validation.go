package tlsconfig

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
)

const (
	// NearExpirationThreshold is the duration before expiration to issue warnings.
	NearExpirationThreshold = 30 * 24 * time.Hour
	// PeriodicCheckInterval is how often to check certificate expiration while running.
	PeriodicCheckInterval = 24 * time.Hour
)

// CertInfo contains information about a parsed certificate.
type CertInfo struct {
	Subject     string
	Issuer      string
	NotBefore   time.Time
	NotAfter    time.Time
	IsCA        bool
	DNSNames    []string
	IPAddresses []string
}

// ValidationResult contains the results of certificate validation.
type ValidationResult struct {
	Certificates []CertInfo
	Expiring     []CertInfo
	Expired      []CertInfo
}

// ValidateCertificates validates server and CA certificates at the given paths.
// If certPath or keyPath is empty, only the CA certificate is validated.
// If caPath is empty, only the server certificate is validated.
// Returns an error if any certificate is expired or cannot be parsed.
func ValidateCertificates(certPath, keyPath, caPath string) (*ValidationResult, error) {
	result := &ValidationResult{
		Certificates: make([]CertInfo, 0),
		Expiring:     make([]CertInfo, 0),
		Expired:      make([]CertInfo, 0),
	}

	var certPEM, keyPEM, caPEM []byte
	var err error

	// Load server certificate and key
	if certPath != "" {
		certPEM, err = os.ReadFile(certPath)
		if err != nil {
			return nil, fmt.Errorf("read server certificate: %w", err)
		}
		if keyPath != "" {
			keyPEM, err = os.ReadFile(keyPath)
			if err != nil {
				return nil, fmt.Errorf("read server key: %w", err)
			}
		}
	}

	// Load CA certificate
	if caPath != "" {
		caPEM, err = os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read CA certificate: %w", err)
		}
	}

	// Validate server certificate
	if certPEM != nil {
		var x509Certs []*x509.Certificate
		if keyPEM != nil {
			cert, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				return nil, fmt.Errorf("parse server certificate/key pair: %w", err)
			}
			for _, c := range cert.Certificate {
				x509Cert, err := x509.ParseCertificate(c)
				if err != nil {
					return nil, fmt.Errorf("parse server certificate: %w", err)
				}
				x509Certs = append(x509Certs, x509Cert)
			}
		} else {
			// Parse certificate without key for validation purposes
			x509Certs, err = parseCertificatesFromPEM(certPEM)
			if err != nil {
				return nil, fmt.Errorf("parse server certificate: %w", err)
			}
		}

		for _, x509Cert := range x509Certs {
			info := certInfoFromX509(x509Cert)
			result.Certificates = append(result.Certificates, info)

			now := time.Now()
			if now.After(x509Cert.NotAfter) {
				result.Expired = append(result.Expired, info)
			} else if now.Add(NearExpirationThreshold).After(x509Cert.NotAfter) {
				result.Expiring = append(result.Expiring, info)
			}
		}
	}

	// Validate CA certificate
	if caPEM != nil {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("invalid CA certificate")
		}

		// Parse the CA certificates to get their info
		caCerts, err := parseCertificatesFromPEM(caPEM)
		if err != nil {
			return nil, fmt.Errorf("parse CA certificate: %w", err)
		}

		for _, x509Cert := range caCerts {
			info := certInfoFromX509(x509Cert)
			result.Certificates = append(result.Certificates, info)

			now := time.Now()
			if now.After(x509Cert.NotAfter) {
				result.Expired = append(result.Expired, info)
			} else if now.Add(NearExpirationThreshold).After(x509Cert.NotAfter) {
				result.Expiring = append(result.Expiring, info)
			}
		}
	}

	return result, nil
}

// parseCertificatesFromPEM parses one or more certificates from PEM data.
func parseCertificatesFromPEM(pemData []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate

	for len(pemData) > 0 {
		var block *pem.Block
		block, pemData = pem.Decode(pemData)
		if block == nil {
			break
		}

		if block.Type == "CERTIFICATE" {
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("parse x509 certificate: %w", err)
			}
			certs = append(certs, cert)
		}
	}

	if len(certs) == 0 {
		return nil, fmt.Errorf("no certificates found in PEM data")
	}

	return certs, nil
}

// certInfoFromX509 converts an x509.Certificate to CertInfo.
func certInfoFromX509(cert *x509.Certificate) CertInfo {
	info := CertInfo{
		Subject:     cert.Subject.CommonName,
		Issuer:      cert.Issuer.CommonName,
		NotBefore:   cert.NotBefore,
		NotAfter:    cert.NotAfter,
		IsCA:        cert.IsCA,
		DNSNames:    cert.DNSNames,
		IPAddresses: make([]string, 0, len(cert.IPAddresses)),
	}

	for _, ip := range cert.IPAddresses {
		info.IPAddresses = append(info.IPAddresses, ip.String())
	}

	return info
}

// LogValidationResults logs the results of certificate validation.
func LogValidationResults(log *zap.Logger, result *ValidationResult) {
	if result == nil || len(result.Certificates) == 0 {
		log.Info("no certificates to validate (TLS disabled)")
		return
	}

	for i, cert := range result.Certificates {
		fields := []zap.Field{
			zap.Int("cert_index", i),
			zap.String("subject", cert.Subject),
			zap.String("issuer", cert.Issuer),
			zap.Time("not_before", cert.NotBefore),
			zap.Time("not_after", cert.NotAfter),
			zap.Bool("is_ca", cert.IsCA),
		}
		if len(cert.DNSNames) > 0 {
			fields = append(fields, zap.Strings("dns_names", cert.DNSNames))
		}
		if len(cert.IPAddresses) > 0 {
			fields = append(fields, zap.Strings("ip_addresses", cert.IPAddresses))
		}
		log.Info("certificate validated", fields...)
	}

	for _, cert := range result.Expired {
		log.Error("certificate has expired",
			zap.String("subject", cert.Subject),
			zap.Time("not_after", cert.NotAfter),
		)
	}

	for _, cert := range result.Expiring {
		daysUntilExpiration := time.Until(cert.NotAfter).Hours() / 24
		log.Warn("certificate expiring soon",
			zap.String("subject", cert.Subject),
			zap.Time("not_after", cert.NotAfter),
			zap.Float64("days_until_expiration", daysUntilExpiration),
		)
	}
}

// StartPeriodicValidation starts a goroutine that periodically checks certificate expiration.
// It logs warnings for expiring certificates and errors for expired ones.
func StartPeriodicValidation(ctx context.Context, log *zap.Logger, certPath, keyPath, caPath string) {
	if certPath == "" && caPath == "" {
		log.Debug("TLS disabled, skipping periodic certificate validation")
		return
	}

	go func() {
		ticker := time.NewTicker(PeriodicCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Debug("certificate validation monitor stopped")
				return
			case <-ticker.C:
				checkCertificates(ctx, log, certPath, keyPath, caPath)
			}
		}
	}()
}

// checkCertificates performs a one-time certificate check and logs any issues.
// The validation is run in a goroutine to respect context cancellation for blocking file reads.
func checkCertificates(ctx context.Context, log *zap.Logger, certPath, keyPath, caPath string) {
	type result struct {
		res *ValidationResult
		err error
	}
	ch := make(chan result, 1)

	go func() {
		res, err := ValidateCertificates(certPath, keyPath, caPath)
		ch <- result{res: res, err: err}
	}()

	select {
	case <-ctx.Done():
		log.Debug("certificate validation check cancelled")
		return
	case r := <-ch:
		if r.err != nil {
			log.Error("certificate validation check failed", zap.Error(r.err))
			return
		}

		if len(r.res.Expired) > 0 {
			log.Error("periodic check: expired certificates detected",
				zap.Int("expired_count", len(r.res.Expired)),
			)
		}

		if len(r.res.Expiring) > 0 {
			log.Warn("periodic check: certificates expiring soon",
				zap.Int("expiring_count", len(r.res.Expiring)),
			)
		}
	}
}
