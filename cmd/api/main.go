package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"taskq/internal/queue"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// jobResponse is what callers see. It deliberately omits current_worker,
// lease_expiry and fencing_token: those are claiming internals, and anything
// returned here becomes something callers depend on.
type jobResponse struct {
	ID          int64   `json:"id"`
	Type        string  `json:"type"`
	State       string  `json:"state"`
	Attempts    int64   `json:"attempts"`
	MaxAttempts int64   `json:"max_attempts"`
	LastError   *string `json:"last_error"`
}

func main() {
	ctx := context.Background()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	q := queue.NewQueue(pool)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Type    string         `json:"type"`
			Payload map[string]any `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		if body.Type == "" {
			http.Error(w, "type is required", http.StatusBadRequest)
			return
		}

		id, created, err := q.EnqueueAt(body.Type, body.Payload, time.Now(), r.Header.Get("Idempotency-Key"), queue.PriorityNormal)
		if errors.Is(err, queue.ErrKeyReused) {
			http.Error(w, "Idempotency-Key already used for a different job", http.StatusConflict)
			return
		}
		if err != nil {
			log.Printf("Failed to enqueue job: %v", err)
			http.Error(w, "failed to enqueue job", http.StatusInternalServerError)
			return
		}

		// A retry that hits an existing key is the mechanism working, so it
		// gets the original job's id rather than an error.
		status := http.StatusCreated
		if !created {
			status = http.StatusOK
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]int64{"id": id})
	})

	mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "id must be a number", http.StatusBadRequest)
			return
		}

		status, ok, err := q.Get(id)
		if err != nil {
			log.Printf("Failed to fetch job %d: %v", id, err)
			http.Error(w, "failed to fetch job", http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jobResponse{
			ID:          status.ID,
			Type:        status.Type,
			State:       status.State,
			Attempts:    status.Attempts,
			MaxAttempts: status.MaxAttempts,
			LastError:   status.LastError,
		})
	})

	log.Println("Listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
