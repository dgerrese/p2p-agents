package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "usage: p2p-agents <command>")
	os.Exit(2)
}
