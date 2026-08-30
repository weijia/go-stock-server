
package main

import (
	"fmt"
	"os"

	"go-stock-server/internal/core"
)

func main() {
	if _, err := core.RunLegacyMain(os.Args[1:], true); err != nil {
		fmt.Fprintf(os.Stderr, "启动失败: %v\n", err)
		os.Exit(1)
	}
}
