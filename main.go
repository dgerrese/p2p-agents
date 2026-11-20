package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// runMCP is the no-args MCP server. Task 7 replaces this stub.
func runMCP(ctx context.Context) error { return errors.New("not implemented") }

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	var err error
	if len(os.Args) < 2 {
		err = runMCP(ctx)
	} else {
		err = runCLI(ctx, os.Args[1:], os.Stdin, os.Stdout)
	}
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "p2p-agents:", err)
		os.Exit(1)
	}
}
