package version_test

import (
	"testing"

	"github.com/zhavkk/Diploma/pkg/version"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		want     version.PGVersion
		wantZero bool
	}{
		{
			name:  "full version string",
			input: "PostgreSQL 16.0",
			want:  version.PGVersion{Major: 16, Minor: 0, Patch: 0},
		},
		{
			name:  "full version with patch",
			input: "PostgreSQL 15.2.3",
			want:  version.PGVersion{Major: 15, Minor: 2, Patch: 3},
		},
		{
			name:  "version with platform info",
			input: "PostgreSQL 15.2 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 11.3.0, 64-bit",
			want:  version.PGVersion{Major: 15, Minor: 2, Patch: 0},
		},
		{
			name:  "version without PostgreSQL prefix",
			input: "15.3",
			want:  version.PGVersion{Major: 15, Minor: 3, Patch: 0},
		},
		{
			name:  "version major only",
			input: "PostgreSQL 16",
			want:  version.PGVersion{Major: 16, Minor: 0, Patch: 0},
		},
		{
			name:  "version major only without prefix",
			input: "14",
			want:  version.PGVersion{Major: 14, Minor: 0, Patch: 0},
		},
		{
			name:  "version with trailing text",
			input: "16.1-beta1",
			want:  version.PGVersion{Major: 16, Minor: 1, Patch: 0},
		},
		{
			name:     "empty string",
			input:    "",
			wantZero: true,
		},
		{
			name:     "invalid format",
			input:    "invalid version",
			wantZero: true,
		},
		{
			name:     "no numbers",
			input:    "PostgreSQL",
			wantZero: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := version.Parse(tt.input)
			if tt.wantZero {
				if !got.IsZero() {
					t.Errorf("Parse(%q) = %+v, want zero version", tt.input, got)
				}
				return
			}
			if got.Major != tt.want.Major {
				t.Errorf("Parse(%q).Major = %d, want %d", tt.input, got.Major, tt.want.Major)
			}
			if got.Minor != tt.want.Minor {
				t.Errorf("Parse(%q).Minor = %d, want %d", tt.input, got.Minor, tt.want.Minor)
			}
			if got.Patch != tt.want.Patch {
				t.Errorf("Parse(%q).Patch = %d, want %d", tt.input, got.Patch, tt.want.Patch)
			}
		})
	}
}

func TestString(t *testing.T) {
	v := version.PGVersion{Major: 15, Minor: 2, Patch: 3}
	want := "15.2.3"
	if got := v.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestStringMajorMinor(t *testing.T) {
	v := version.PGVersion{Major: 15, Minor: 2, Patch: 3}
	want := "15.2"
	if got := v.StringMajorMinor(); got != want {
		t.Errorf("StringMajorMinor() = %q, want %q", got, want)
	}
}

func TestCompare(t *testing.T) {
	tests := []struct {
		name  string
		v     version.PGVersion
		other version.PGVersion
		want  int
	}{
		{"equal", version.PGVersion{15, 2, 0}, version.PGVersion{15, 2, 0}, 0},
		{"less major", version.PGVersion{14, 9, 9}, version.PGVersion{15, 0, 0}, -1},
		{"greater major", version.PGVersion{16, 0, 0}, version.PGVersion{15, 9, 9}, 1},
		{"less minor", version.PGVersion{15, 1, 9}, version.PGVersion{15, 2, 0}, -1},
		{"greater minor", version.PGVersion{15, 3, 0}, version.PGVersion{15, 2, 9}, 1},
		{"less patch", version.PGVersion{15, 2, 0}, version.PGVersion{15, 2, 1}, -1},
		{"greater patch", version.PGVersion{15, 2, 5}, version.PGVersion{15, 2, 3}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.v.Compare(tt.other); got != tt.want {
				t.Errorf("Compare() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestIsZero(t *testing.T) {
	tests := []struct {
		name  string
		v     version.PGVersion
		want  bool
	}{
		{"zero version", version.PGVersion{}, true},
		{"explicit zero", version.PGVersion{0, 0, 0}, true},
		{"non-zero major", version.PGVersion{1, 0, 0}, false},
		{"non-zero minor", version.PGVersion{0, 1, 0}, false},
		{"non-zero patch", version.PGVersion{0, 0, 1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.v.IsZero(); got != tt.want {
				t.Errorf("IsZero() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompatibleWith(t *testing.T) {
	tests := []struct {
		name  string
		v     version.PGVersion
		other version.PGVersion
		want  bool
	}{
		{"identical versions", version.PGVersion{16, 0, 0}, version.PGVersion{16, 0, 0}, true},
		{"patch difference", version.PGVersion{15, 2, 0}, version.PGVersion{15, 2, 3}, true},
		{"minor version +1", version.PGVersion{16, 1, 0}, version.PGVersion{16, 0, 0}, true},
		{"minor version -1", version.PGVersion{16, 0, 0}, version.PGVersion{16, 1, 0}, true},
		{"minor version +2", version.PGVersion{16, 2, 0}, version.PGVersion{16, 0, 0}, true}, // now true per PostgreSQL docs
		{"minor version -2", version.PGVersion{16, 0, 0}, version.PGVersion{16, 2, 0}, true}, // now true per PostgreSQL docs
		{"large minor difference (5)", version.PGVersion{15, 5, 0}, version.PGVersion{15, 0, 0}, true},
		{"large minor difference (10)", version.PGVersion{14, 10, 0}, version.PGVersion{14, 0, 0}, true},
		{"major version difference", version.PGVersion{15, 2, 0}, version.PGVersion{16, 0, 0}, false},
		{"zero version", version.PGVersion{0, 0, 0}, version.PGVersion{15, 2, 0}, false},
		{"other zero version", version.PGVersion{15, 2, 0}, version.PGVersion{0, 0, 0}, false},
		{"both zero versions", version.PGVersion{0, 0, 0}, version.PGVersion{0, 0, 0}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.v.CompatibleWith(tt.other); got != tt.want {
				t.Errorf("CompatibleWith() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecommendedCompatibleWith(t *testing.T) {
	tests := []struct {
		name  string
		v     version.PGVersion
		other version.PGVersion
		want  bool
	}{
		{"identical versions", version.PGVersion{16, 0, 0}, version.PGVersion{16, 0, 0}, true},
		{"patch difference", version.PGVersion{15, 2, 0}, version.PGVersion{15, 2, 3}, true},
		{"minor version +1", version.PGVersion{16, 1, 0}, version.PGVersion{16, 0, 0}, true},
		{"minor version -1", version.PGVersion{16, 0, 0}, version.PGVersion{16, 1, 0}, true},
		{"minor version +2", version.PGVersion{16, 2, 0}, version.PGVersion{16, 0, 0}, false}, // not recommended
		{"minor version -2", version.PGVersion{16, 0, 0}, version.PGVersion{16, 2, 0}, false}, // not recommended
		{"large minor difference (5)", version.PGVersion{15, 5, 0}, version.PGVersion{15, 0, 0}, false},
		{"major version difference", version.PGVersion{15, 2, 0}, version.PGVersion{16, 0, 0}, false},
		{"zero version", version.PGVersion{0, 0, 0}, version.PGVersion{15, 2, 0}, false},
		{"other zero version", version.PGVersion{15, 2, 0}, version.PGVersion{0, 0, 0}, false},
		{"both zero versions", version.PGVersion{0, 0, 0}, version.PGVersion{0, 0, 0}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.v.RecommendedCompatibleWith(tt.other); got != tt.want {
				t.Errorf("RecommendedCompatibleWith() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMinVersionForReplication(t *testing.T) {
	tests := []struct {
		name  string
		v     version.PGVersion
		want  version.PGVersion
	}{
		{"standard version", version.PGVersion{16, 1, 0}, version.PGVersion{16, 0, 0}},
		{"high minor version", version.PGVersion{15, 10, 0}, version.PGVersion{15, 0, 0}},
		{"zero minor", version.PGVersion{15, 0, 0}, version.PGVersion{15, 0, 0}},
		{"with patch", version.PGVersion{14, 2, 5}, version.PGVersion{14, 0, 0}},
		{"zero version", version.PGVersion{0, 0, 0}, version.PGVersion{0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.v.MinVersionForReplication()
			if got.Major != tt.want.Major || got.Minor != tt.want.Minor || got.Patch != tt.want.Patch {
				t.Errorf("MinVersionForReplication() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestMaxVersionForReplication(t *testing.T) {
	tests := []struct {
		name  string
		v     version.PGVersion
		want  version.PGVersion
	}{
		{"standard version", version.PGVersion{16, 1, 0}, version.PGVersion{16, 999, 0}},
		{"zero minor", version.PGVersion{15, 0, 0}, version.PGVersion{15, 999, 0}},
		{"high minor version", version.PGVersion{14, 100, 0}, version.PGVersion{14, 999, 0}},
		{"zero version", version.PGVersion{0, 0, 0}, version.PGVersion{0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.v.MaxVersionForReplication()
			if got.Major != tt.want.Major || got.Minor != tt.want.Minor || got.Patch != tt.want.Patch {
				t.Errorf("MaxVersionForReplication() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
