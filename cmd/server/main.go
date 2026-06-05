package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"
	"temporal-lite/examples"
	"temporal-lite/internal/api"
	"temporal-lite/internal/engine"
	"temporal-lite/internal/store"
	"temporal-lite/internal/worker"

	_ "github.com/lib/pq"
)

func main() {
	dbConn := getEnv("DB_CONN", "postgres://postgres:postgres@localhost:5432/temporal_lite?sslmode=disable")
	port := getEnv("PORT", "8132")
	migrationsDir := getEnv("MIGRATIONS_DIR", "./migrations")

	if err := runMigrations(dbConn, migrationsDir); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	s, err := store.NewPostgresStore(dbConn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer s.Close()

	e := engine.NewEngine(s)

	examples.RegisterWorkflows(e)
	examples.RegisterActivities(e)

	w := worker.NewWorker(s, e, "default", 10)
	go w.Start()

	server := api.NewServer(e, s, ":"+port)

	go func() {
		log.Printf("Temporal Lite server starting on port %s...", port)
		if err := server.Start(); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	w.Stop()
	_ = server.Stop(context.Background())
	log.Println("Server stopped gracefully")
}

func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func runMigrations(dbConn, migrationsDir string) error {
	db, err := sql.Open("postgres", dbConn)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("create migrations table: %w", err)
	}

	files, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	for _, f := range files {
		if filepath.Ext(f.Name()) != ".sql" {
			continue
		}

		version := f.Name()
		var exists bool
		err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)", version).Scan(&exists)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", version, err)
		}

		if exists {
			log.Printf("Migration %s already applied, skipping", version)
			continue
		}

		content, err := os.ReadFile(filepath.Join(migrationsDir, f.Name()))
		if err != nil {
			return fmt.Errorf("read migration %s: %w", version, err)
		}

		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin transaction: %w", err)
		}

		if _, err := tx.Exec(string(content)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("execute migration %s: %w", version, err)
		}

		if _, err := tx.Exec("INSERT INTO schema_migrations (version) VALUES ($1)", version); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %s: %w", version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", version, err)
		}

		log.Printf("Applied migration: %s", version)
	}

	log.Println("All migrations applied successfully")
	return nil
}
