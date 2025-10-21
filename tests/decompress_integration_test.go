package tests

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDecompressIntegrationSmall_Blackbox(t *testing.T) {
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "payload.bin")
	content := make([]byte, 1024)
	for i := range content {
		content[i] = byte(i % 256)
	}
	if err := os.WriteFile(src, content, fs.ModePerm); err != nil {
		t.Fatalf("write payload failed: %v", err)
	}
	// compress via test-tool
	dstS2 := filepath.Join(tmpDir, "payload.bin.s2")
	cmd := exec.Command("go", "run", "-tags=testtools", "..", "test-tool", "compress", src, dstS2)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("compress failed: %v", err)
	}
	// decompress
	out := filepath.Join(tmpDir, "payload.out")
	cmd = exec.Command("go", "run", "-tags=testtools", "..", "test-tool", "decompress", dstS2, out)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("decompress failed: %v", err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read out failed: %v", err)
	}
	if len(got) != len(content) {
		t.Fatalf("length mismatch: got %d want %d", len(got), len(content))
	}
	for i := range got {
		if got[i] != content[i] {
			t.Fatalf("byte mismatch at %d: got %d want %d", i, got[i], content[i])
		}
	}
}
