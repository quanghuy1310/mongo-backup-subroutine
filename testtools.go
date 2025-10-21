//go:build testtools
// +build testtools

package main

import (
    "fmt"
    "os"
    "log"
)

// This file is compiled only when using -tags=testtools and provides a tiny
// CLI entrypoint `test-tool` that tests can invoke via `go run -tags=testtools .`.
// It avoids exposing internal helpers in production builds.

func main_testtool() {
    // placeholder to allow `go run` to build package
}

func init() {
    // If executed as main with arguments starting with "test-tool", run helper.
    if len(os.Args) >= 2 && os.Args[1] == "test-tool" {
        if len(os.Args) < 5 {
            fmt.Fprintf(os.Stderr, "usage: test-tool compress|decompress <src> <dst>\n")
            os.Exit(2)
        }
        cmd := os.Args[2]
        src := os.Args[3]
        dst := os.Args[4]
        // ensure loggers are initialized for test mode
        if Info == nil || Warn == nil || Error == nil {
            Info = log.New(os.Stdout, "[INFO] ", log.LstdFlags)
            Warn = log.New(os.Stdout, "[WARN] ", log.LstdFlags)
            Error = log.New(os.Stderr, "[ERROR] ", log.LstdFlags)
        }

        var err error
        switch cmd {
        case "compress":
            m := map[string]string{src: dst}
            err = CompressFilesS2(m)
        case "decompress":
            err = DecompressFileS2(src, dst)
        default:
            fmt.Fprintf(os.Stderr, "unknown test-tool command: %s\n", cmd)
            os.Exit(2)
        }
        if err != nil {
            fmt.Fprintf(os.Stderr, "test-tool error: %v\n", err)
            os.Exit(1)
        }
        fmt.Fprintf(os.Stdout, "ok\n")
        os.Exit(0)
    }
}
