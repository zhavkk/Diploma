package version

import (
	"fmt"
	"regexp"
	"strconv"
)

var (
	// VersionRegex extracts major, minor, and patch numbers from PostgreSQL version strings.
	// Examples: "PostgreSQL 16.0", "PostgreSQL 15.2 on x86_64-pc-linux-gnu", "15.3", "16"
	// Also handles leading whitespace: " PostgreSQL 16.13 (Debian...)"
	VersionRegex = regexp.MustCompile(`^\s*(?:PostgreSQL\s*)?(\d+)(?:\.(\d+))?(?:\.(\d+))?`)
)

// PGVersion represents a parsed PostgreSQL version with major, minor, and patch components.
type PGVersion struct {
	Major int
	Minor int
	Patch int
}

// Parse extracts the PostgreSQL version from a version string.
// The input can be in various formats:
//   - "PostgreSQL 16.0"
//   - "PostgreSQL 15.2 on x86_64-pc-linux-gnu"
//   - "15.3"
//   - "16"
// Returns zero version if parsing fails.
func Parse(versionStr string) PGVersion {
	matches := VersionRegex.FindStringSubmatch(versionStr)
	if len(matches) < 2 {
		return PGVersion{}
	}

	major, _ := strconv.Atoi(matches[1])
	minor := 0
	if len(matches) > 2 && matches[2] != "" {
		minor, _ = strconv.Atoi(matches[2])
	}
	patch := 0
	if len(matches) > 3 && matches[3] != "" {
		patch, _ = strconv.Atoi(matches[3])
	}

	return PGVersion{Major: major, Minor: minor, Patch: patch}
}

// String returns the canonical version string "major.minor.patch".
func (v PGVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// StringMajorMinor returns "major.minor" format for compatibility checks.
func (v PGVersion) StringMajorMinor() string {
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

// Compare returns -1 if v < other, 1 if v > other, 0 if equal.
// Comparison is done by major, then minor, then patch.
func (v PGVersion) Compare(other PGVersion) int {
	if v.Major != other.Major {
		if v.Major < other.Major {
			return -1
		}
		return 1
	}
	if v.Minor != other.Minor {
		if v.Minor < other.Minor {
			return -1
		}
		return 1
	}
	if v.Patch != other.Patch {
		if v.Patch < other.Patch {
			return -1
		}
		return 1
	}
	return 0
}

// IsZero returns true if all version components are zero.
func (v PGVersion) IsZero() bool {
	return v.Major == 0 && v.Minor == 0 && v.Patch == 0
}

// CompatibleWith checks if this version is compatible with another version for replication.
// PostgreSQL allows replication between different minor versions within the same major version.
// According to PostgreSQL documentation (Streaming Replication section), physical replication
// works between any minor versions within the same major version.
//
// Rules (based on PostgreSQL documentation):
//   - Major versions must match exactly
//   - Minor versions within the same major version are compatible regardless of difference
//   - Patch versions are ignored for compatibility
//
// Returns true if compatible, false otherwise.
func (v PGVersion) CompatibleWith(other PGVersion) bool {
	if v.IsZero() || other.IsZero() {
		return false
	}

	// Major versions must match exactly
	if v.Major != other.Major {
		return false
	}

	// Any minor version within the same major is compatible per PostgreSQL docs
	return true
}

// RecommendedCompatibleWith checks if this version is recommended to be compatible with another
// version for replication. While PostgreSQL supports any minor version difference within the
// same major version, the PostgreSQL documentation advises keeping versions "as close as possible".
//
// Returns true for identical versions or minor version differences of 0 or 1.
// Returns false for larger minor differences (while still technically compatible).
func (v PGVersion) RecommendedCompatibleWith(other PGVersion) bool {
	if v.IsZero() || other.IsZero() {
		return false
	}

	// Major versions must match exactly
	if v.Major != other.Major {
		return false
	}

	// Recommended: minor version difference should not exceed 1
	minorDiff := v.Minor - other.Minor
	if minorDiff < -1 || minorDiff > 1 {
		return false
	}

	return true
}

// MinVersionForReplication returns the minimum version that can be a replica for this version.
// Since PostgreSQL allows any minor version within the same major version, this returns
// version X.0.0 for any X.Y.Z.
func (v PGVersion) MinVersionForReplication() PGVersion {
	if v.Major == 0 {
		return v
	}
	return PGVersion{Major: v.Major, Minor: 0, Patch: 0}
}

// MaxVersionForReplication returns the maximum version that can be a replica for this version.
// Since PostgreSQL allows any minor version within the same major version, this returns
// version X.999.0 to represent the theoretical maximum minor version.
func (v PGVersion) MaxVersionForReplication() PGVersion {
	if v.Major == 0 {
		return v
	}
	return PGVersion{Major: v.Major, Minor: 999, Patch: 0}
}
