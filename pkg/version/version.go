package version

import (
	"fmt"
	"regexp"
	"strconv"
)

var (
	VersionRegex = regexp.MustCompile(`^\s*(?:PostgreSQL\s*)?(\d+)(?:\.(\d+))?(?:\.(\d+))?`)
)

type PGVersion struct {
	Major int
	Minor int
	Patch int
}

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

func (v PGVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

func (v PGVersion) StringMajorMinor() string {
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

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

func (v PGVersion) IsZero() bool {
	return v.Major == 0 && v.Minor == 0 && v.Patch == 0
}

func (v PGVersion) CompatibleWith(other PGVersion) bool {
	if v.IsZero() || other.IsZero() {
		return false
	}

	if v.Major != other.Major {
		return false
	}

	return true
}

func (v PGVersion) RecommendedCompatibleWith(other PGVersion) bool {
	if v.IsZero() || other.IsZero() {
		return false
	}

	if v.Major != other.Major {
		return false
	}

	minorDiff := v.Minor - other.Minor
	if minorDiff < -1 || minorDiff > 1 {
		return false
	}

	return true
}

func (v PGVersion) MinVersionForReplication() PGVersion {
	if v.Major == 0 {
		return v
	}
	return PGVersion{Major: v.Major, Minor: 0, Patch: 0}
}

func (v PGVersion) MaxVersionForReplication() PGVersion {
	if v.Major == 0 {
		return v
	}
	return PGVersion{Major: v.Major, Minor: 999, Patch: 0}
}
