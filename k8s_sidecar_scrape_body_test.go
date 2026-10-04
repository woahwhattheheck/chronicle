package chronicle

import (
	"strings"
	"testing"
)

func TestReadSidecarScrapeBodyLimit(t *testing.T) {
	const limit int64 = 4

	body, err := readSidecarScrapeBody(strings.NewReader("abcd"), limit)
	if err != nil {
		t.Fatalf("exact-limit body rejected: %v", err)
	}
	if string(body) != "abcd" {
		t.Fatalf("exact-limit body = %q, want %q", body, "abcd")
	}

	if _, err := readSidecarScrapeBody(strings.NewReader("abcde"), limit); err == nil {
		t.Fatal("one-byte-oversize body was accepted")
	}
}
