//go:build windows

package chronicle

import (
	"path/filepath"
	"testing"
)

func TestValidateExportPathWindows(t *testing.T) {
	// Relative paths resolve beneath an ordinary user-owned directory, not a
	// drive root. A directory called etc here is not the sensitive root etc.
	workDir := t.TempDir()
	t.Chdir(workDir)

	tests := []struct {
		name      string
		path      string
		wantError bool
	}{
		{"drive root", `C:\etc`, true},
		{"other drive descendant", `D:\etc\passwd`, true},
		{"case insensitive", `c:\EtC\passwd`, true},
		{"forward slashes", `C:/UsR/BiN/tool.csv`, true},
		{"mixed separators", `C:\usr/sbin\tool.csv`, true},
		{"root relative", `\etc\passwd`, true},
		{"dot segments", `C:\safe\..\ROOT\keys.csv`, true},
		{"UNC root", `\\server\share\etc`, true},
		{"UNC descendant", `\\SERVER\SHARE\UsR\BiN\tool.csv`, true},
		{"extended drive", `\\?\C:\EtC\passwd`, true},
		{"extended UNC", `\\?\UNC\server\share\UsR\BiN\tool.csv`, true},
		{"device drive", `\\.\C:\etc\passwd`, true},
		{"sibling prefix", `C:\etcetera\file.csv`, false},
		{"nested sibling prefix", `C:\usr\binutils\file.csv`, false},
		{"ordinary nested directory", `C:\Users\Person\etc\File.csv`, false},
		{"relative directory", `etc\File.csv`, false},
		{"relative mixed case", `ROOT/Data.csv`, false},
		{"drive relative directory", filepath.VolumeName(workDir) + `etc\File.csv`, false},
		{"UNC share name", `\\server\etc\File.csv`, false},
		{"UNC sibling prefix", `\\server\share\usr\binary\File.csv`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateExportPath(tt.path)
			if tt.wantError {
				if err == nil {
					t.Errorf("expected error for path %q, got %q", tt.path, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for path %q: %v", tt.path, err)
			}
			want, err := filepath.Abs(filepath.Clean(tt.path))
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Errorf("resolved path spelling changed: got %q, want %q", got, want)
			}
		})
	}
}
