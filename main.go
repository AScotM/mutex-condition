package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	Requests  int64
	Errors    int64
	BytesSent int64
}

func (m *Metrics) AddRequest(bytes int64, failed bool) {
	atomic.AddInt64(&m.Requests, 1)
	atomic.AddInt64(&m.BytesSent, bytes)
	if failed {
		atomic.AddInt64(&m.Errors, 1)
	}
}

func (m *Metrics) Snapshot() (int64, int64, int64) {
	return atomic.LoadInt64(&m.Requests), atomic.LoadInt64(&m.Errors), atomic.LoadInt64(&m.BytesSent)
}

type Logger struct {
	mu       sync.Mutex
	Messages []string
}

func (l *Logger) Log(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Messages = append(l.Messages, msg)
}

func (l *Logger) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.Messages)
}

type UserDB struct {
	mu    sync.Mutex
	Users map[int]string
}

func (db *UserDB) Add(id int, name string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.Users[id] = name
}

func (db *UserDB) Count() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return len(db.Users)
}

type Server struct {
	Metrics *Metrics
	Logger  *Logger
	UserDB  *UserDB
}

type ServerSnapshot struct {
	Requests  int64
	Errors    int64
	BytesSent int64
	LogCount  int
	UserCount int
}

func (s *Server) Snapshot() ServerSnapshot {
	req, errs, bytes := s.Metrics.Snapshot()
	return ServerSnapshot{
		Requests:  req,
		Errors:    errs,
		BytesSent: bytes,
		LogCount:  s.Logger.Count(),
		UserCount: s.UserDB.Count(),
	}
}

func worker(id int, srv *Server, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < 1000; i++ {
		bytes := int64(rand.Intn(10000))
		failed := rand.Intn(10) == 0

		srv.Metrics.AddRequest(bytes, failed)

		srv.Logger.Log(fmt.Sprintf("worker=%d request=%d", id, i))

		if rand.Intn(20) == 0 {
			userID := rand.Intn(1000)
			srv.UserDB.Add(userID, fmt.Sprintf("user-%d", userID))
		}

		time.Sleep(time.Millisecond)
	}
}

func monitor(srv *Server, ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			snap := srv.Snapshot()
			fmt.Println("----- STATUS -----")
			fmt.Println("Requests :", snap.Requests)
			fmt.Println("Errors   :", snap.Errors)
			fmt.Println("Bytes    :", snap.BytesSent)
			fmt.Println("Logs     :", snap.LogCount)
			fmt.Println("Users    :", snap.UserCount)
			fmt.Println()

		case <-ctx.Done():
			return
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	metrics := &Metrics{}
	logger := &Logger{
		Messages: make([]string, 0),
	}
	userDB := &UserDB{
		Users: make(map[int]string),
	}

	server := &Server{
		Metrics: metrics,
		Logger:  logger,
		UserDB:  userDB,
	}

	ctx, cancel := context.WithCancel(context.Background())

	go monitor(server, ctx)

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go worker(i, server, &wg)
	}

	wg.Wait()

	cancel()

	time.Sleep(100 * time.Millisecond)

	snap := server.Snapshot()

	fmt.Println("===== FINAL =====")
	fmt.Println("Requests :", snap.Requests)
	fmt.Println("Errors   :", snap.Errors)
	fmt.Println("Bytes    :", snap.BytesSent)
	fmt.Println("Logs     :", snap.LogCount)
	fmt.Println("Users    :", snap.UserCount)
}
