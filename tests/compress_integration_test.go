package tests

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestCompressDecompressIntegration_Blackbox(t *testing.T) {
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "data.txt")
	content := []byte("hello world from integration test\n")
	if err := os.WriteFile(src, content, fs.ModePerm); err != nil {
		t.Fatalf("write src failed: %v", err)
	}
	// compress via test-tool
	dsts2 := src + ".s2"
	cmd := exec.Command("go", "run", "-tags=testtools", "..", "test-tool", "compress", src, dsts2)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("compress failed: %v", err)
	}
	// decompress
	dec := filepath.Join(tmpDir, "data.out")
	cmd = exec.Command("go", "run", "-tags=testtools", "..", "test-tool", "decompress", dsts2, dec)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
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
