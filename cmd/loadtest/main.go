// cmd/loadtest/main.go---->A dedicated script for measuring worker throughput (Jobs Per Second).
//
// Usage: go run cmd/loadtest/main.go --count=1000
//	or WORKER_CONCURRENCY=50 go run cmd/worker/main.go
//
// How it works:
// 1. FLUSHALL on Redis to start with a clean slate.
// 2. Concurrently blast the API with N jobs using the "sleep" executor (100ms fixed duration).
// 3. Start a high-precision timer.
// 4. Poll the /metrics endpoint until total_processed == N.
// 5. Stop the timer and calculate JPS.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mitudk/distributed-job-scheduler/config"
	"github.com/mitudk/distributed-job-scheduler/internal/store"
)

type MetricsResponse struct {
	TotalProcessed int64 `json:"total_processed"`
	TotalFailed    int64 `json:"total_failed"`
	TotalDead      int64 `json:"total_dead"`
}

func main() {
	countFlag := flag.Int("count", 1000, "Number of jobs to enqueue for the load test")
	flag.Parse() //if user doesnt provide  jobs count in terminal ,default is 1000 jobs.

	count := *countFlag

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	fmt.Println("🚀 Starting Phase 8 Load Test...")
	fmt.Printf("🎯 Target: %d jobs using the 'sleep' executor (100ms fixed duration each)\n", count)

	// 1. Flush Redis
	rdb, err := store.NewRedisClient(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer rdb.Close()

	ctx := context.Background()
	fmt.Print("🧹 Flushing Redis to ensure a clean slate... ")
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		log.Fatalf("Failed to flush redis: %v", err)
	}
	fmt.Println("Done.")

	// 2. Blast the API
	apiURL := fmt.Sprintf("http://localhost:%d", cfg.APIPort)
	fmt.Printf("🔫 Blasting %s/jobs with %d requests... ", apiURL, count)

	var wg sync.WaitGroup
	jobChan := make(chan struct{}, count)
	for i := 0; i < count; i++ {
		jobChan <- struct{}{} ///an object that takes exactly 0 bytes of computer memory
	}
	close(jobChan)

	// Use 50 concurrent goroutines to enqueue jobs  fast.
	concurrency := 50
	var successCount int32

	payload := []byte(`{"name": "sleep", "priority": 1, "payload": {}}`)
	client := &http.Client{Timeout: 5 * time.Second} //client must response within 5 seconds,else it will timeout

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobChan {
				req, _ := http.NewRequest("POST", apiURL+"/jobs", bytes.NewBuffer(payload))
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err == nil && resp.StatusCode == http.StatusAccepted {
					atomic.AddInt32(&successCount, 1)
					io.Copy(io.Discard, resp.Body) //discards the response body ,for performance here we dont care about response from /jobs endpoint. In production, you'd decode the JSON response.
					resp.Body.Close()              //closes the response body to prevent resource leaks.
				} else if resp != nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}()
	}

	wg.Wait()
	fmt.Printf("Done. Successfully enqueued %d jobs.\n", successCount)
	if int(successCount) != count {
		log.Fatalf("Failed to enqueue all jobs. Is the API server running?")
	}

	// 3. Start timer and poll
	fmt.Println("⏱️  Starting timer. Waiting for workers to drain the queue...")
	start := time.Now()

	for {
		resp, err := client.Get(apiURL + "/metrics")
		if err != nil {
			log.Fatalf("Failed to fetch metrics: %v", err)
		}

		var m MetricsResponse
		if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
			resp.Body.Close()
			log.Fatalf("Failed to decode metrics: %v", err)
		}
		resp.Body.Close()

		completed := m.TotalProcessed + m.TotalFailed + m.TotalDead
		if completed >= int64(count) {
			break
		}

		// Print progress every second so the user knows it's working
		fmt.Printf("\rProgress: %d / %d jobs completed", completed, count)
		time.Sleep(250 * time.Millisecond)
	}

	duration := time.Since(start)
	fmt.Printf("\rProgress: %d / %d jobs completed\n\n", count, count)

	// 4. Print results
	jps := float64(count) / duration.Seconds()

	fmt.Println("===========================================")
	fmt.Println("📊 LOAD TEST RESULTS")
	fmt.Println("===========================================")
	fmt.Printf("Total Jobs Processed : %d\n", count)
	fmt.Printf("Total Time Taken     : %.2f seconds\n", duration.Seconds())
	fmt.Printf("Throughput           : %.2f Jobs Per Second (JPS)\n", jps)
	fmt.Println("===========================================")
	fmt.Println("\nTo test scaling, stop the worker, change WORKER_CONCURRENCY, start it again, and re-run this script!")
}
