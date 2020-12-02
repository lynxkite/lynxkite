// Extra Go code for NetworKit integration.
package networkit

import (
	"os"
	"strconv"
)

// Call this before using NetworKit if you want NETWORKIT_THREADS to apply.
func SetThreadsFromEnv() {
	s := os.Getenv("NETWORKIT_THREADS")
	if t, err := strconv.Atoi(s); err == nil {
		SetNumberOfThreads(t)
	}
}
