// Extra Go code for NetworKit integration.
package networkit

import (
	"os"
	"strconv"
)

// Call this before using NetworKit if you want NETWORKIT_THREADS to apply.
func SetThreadsFromEnv() {
	s := os.Getenv("NETWORKIT_THREADS")
	if len(s) > 0 {
		t, err := strconv.Atoi(s)
		if err != nil {
			panic(err)
		}
		SetNumberOfThreads(t)
	}
}
