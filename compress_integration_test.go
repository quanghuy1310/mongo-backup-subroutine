package main

import (
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"testing"
)

// Integration test: create a temp file, compress it, decompress it, compare bytes
func TestCompressDecompressIntegration(t *testing.T) {
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "data.txt")
	content := []byte("hello world from integration test\n")
	if err := os.WriteFile(src, content, fs.ModePerm); err != nil {
		t.Fatalf("write src failed: %v", err)
	}
	// prepare logger for test (avoid nil Info/Warn/Error)
	Info = log.New(os.Stdout, "[INFO] ", log.LstdFlags)
	Warn = log.New(os.Stdout, "[WARN] ", log.LstdFlags)
	Error = log.New(os.Stderr, "[ERROR] ", log.LstdFlags)

	// target s2 paths: compress the source file
	dsts2 := src + ".s2"
	// emulate compress map: src -> src.s2
	m := map[string]string{src: dsts2}
	if err := CompressFilesS2(m); err != nil {
		t.Fatalf("compress failed: %v", err)
	}
	// decompress
	dec := filepath.Join(tmpDir, "data.out")
	if err := DecompressFileS2(dsts2, dec); err != nil {
		t.Fatalf("decompress failed: %v", err)
	}
	got, err := os.ReadFile(dec)
	if err != nil {
		t.Fatalf("read dec failed: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("content mismatch: got %q want %q", string(got), string(content))
	}
}
