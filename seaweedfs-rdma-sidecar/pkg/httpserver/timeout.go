package httpserver

import "time"

func operationTimeout(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return 30 * time.Second
}
