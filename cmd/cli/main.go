package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"temporal-lite/examples"
	"temporal-lite/internal/engine"
	"temporal-lite/internal/model"
	"temporal-lite/internal/store"

	"github.com/urfave/cli/v2"
)

type replayFunc func(ctx *engine.WorkflowContext) error

func main() {
	app := &cli.App{
		Name:  "temporal-lite",
		Usage: "Temporal Lite - A lightweight persistent workflow engine",
		Commands: []*cli.Command{
			{
				Name:    "server",
				Aliases: []string{"s"},
				Usage:   "Start the workflow server",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "db",
						Aliases: []string{"d"},
						Value:   "postgres://postgres:postgres@localhost:5432/temporal_lite?sslmode=disable",
						Usage:   "PostgreSQL connection string",
					},
					&cli.StringFlag{
						Name:    "port",
						Aliases: []string{"p"},
						Value:   "8132",
						Usage:   "Server port",
					},
				},
				Action: func(c *cli.Context) error {
					return runServer(c.String("db"), c.String("port"))
				},
			},
			{
				Name:    "replay",
				Aliases: []string{"r"},
				Usage:   "Replay workflow history for debugging",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "workflow-id",
						Aliases:  []string{"w"},
						Required: true,
						Usage:    "Workflow ID to replay",
					},
					&cli.StringFlag{
						Name:     "run-id",
						Aliases:  []string{"r"},
						Required: true,
						Usage:    "Run ID to replay",
					},
					&cli.StringFlag{
						Name:    "db",
						Aliases: []string{"d"},
						Value:   "postgres://postgres:postgres@localhost:5432/temporal_lite?sslmode=disable",
						Usage:   "PostgreSQL connection string",
					},
					&cli.StringFlag{
						Name:    "events-file",
						Aliases: []string{"f"},
						Usage:   "Path to events JSON file (optional, uses DB if not provided)",
					},
					&cli.BoolFlag{
						Name:    "verbose",
						Aliases: []string{"v"},
						Usage:   "Enable verbose output",
					},
				},
				Action: func(c *cli.Context) error {
					return replayWorkflow(
						c.String("db"),
						c.String("workflow-id"),
						c.String("run-id"),
						c.String("events-file"),
						c.Bool("verbose"),
					)
				},
			},
			{
				Name:    "export-events",
				Aliases: []string{"e"},
				Usage:   "Export workflow events to JSON file",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "workflow-id",
						Aliases:  []string{"w"},
						Required: true,
						Usage:    "Workflow ID",
					},
					&cli.StringFlag{
						Name:     "run-id",
						Aliases:  []string{"r"},
						Required: true,
						Usage:    "Run ID",
					},
					&cli.StringFlag{
						Name:    "db",
						Aliases: []string{"d"},
						Value:   "postgres://postgres:postgres@localhost:5432/temporal_lite?sslmode=disable",
						Usage:   "PostgreSQL connection string",
					},
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Value:   "events.json",
						Usage:   "Output file path",
					},
				},
				Action: func(c *cli.Context) error {
					return exportEvents(
						c.String("db"),
						c.String("workflow-id"),
						c.String("run-id"),
						c.String("output"),
					)
				},
			},
			{
				Name:  "migrate",
				Usage: "Run database migrations",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "db",
						Aliases: []string{"d"},
						Value:   "postgres://postgres:postgres@localhost:5432/temporal_lite?sslmode=disable",
						Usage:   "PostgreSQL connection string",
					},
					&cli.StringFlag{
						Name:    "migrations",
						Aliases: []string{"m"},
						Value:   "./migrations",
						Usage:   "Migrations directory",
					},
				},
				Action: func(c *cli.Context) error {
					return runMigrations(c.String("db"), c.String("migrations"))
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func runServer(dbConn, port string) error {
	fmt.Printf("Starting Temporal Lite server on port %s...\n", port)
	return nil
}

func replayWorkflow(dbConn, workflowID, runID, eventsFile string, verbose bool) error {
	ctx := context.Background()

	var events []*model.Event

	if eventsFile != "" {
		data, err := os.ReadFile(eventsFile)
		if err != nil {
			return fmt.Errorf("read events file: %w", err)
		}
		if err := json.Unmarshal(data, &events); err != nil {
			return fmt.Errorf("parse events: %w", err)
		}
		fmt.Printf("Loaded %d events from file: %s\n", len(events), eventsFile)
	} else {
		s, err := store.NewPostgresStore(dbConn)
		if err != nil {
			return fmt.Errorf("connect to db: %w", err)
		}
		defer s.Close()

		events, err = s.GetAllEvents(ctx, workflowID, runID)
		if err != nil {
			return fmt.Errorf("get events: %w", err)
		}
		fmt.Printf("Loaded %d events from database\n", len(events))
	}

	if verbose {
		fmt.Println("\n=== Event History ===")
		for i, evt := range events {
			fmt.Printf("[%d] %s (event_id: %d, time: %s)\n",
				i, evt.EventType, evt.EventID, evt.Timestamp)
		}
		fmt.Println("====================")
	}

	s, _ := store.NewPostgresStore(dbConn)
	defer s.Close()
	e := engine.NewEngine(s)

	examples.RegisterWorkflows(e)
	examples.RegisterActivities(e)

	fmt.Println("Starting replay...")

	err := e.ReplayWorkflowForDebug(ctx, workflowID, runID, func(wc *engine.WorkflowContext) error {
		fmt.Printf("\nReplay started for WorkflowID: %s, RunID: %s\n", wc.WorkflowID(), wc.RunID())
		fmt.Printf("IsReplaying: %v\n", wc.IsReplaying())

		fn, ok := e.GetWorkflow("OrderWorkflow")
		if !ok {
			return fmt.Errorf("workflow not registered")
		}

		result, err := fn(wc)
		if err != nil {
			fmt.Printf("Replay error: %v\n", err)
			return err
		}

		fmt.Printf("Replay result: %v\n", result)
		return nil
	})

	if err != nil {
		return fmt.Errorf("replay failed: %w", err)
	}

	fmt.Println("\n✅ Replay completed successfully!")
	return nil
}

func exportEvents(dbConn, workflowID, runID, output string) error {
	ctx := context.Background()

	s, err := store.NewPostgresStore(dbConn)
	if err != nil {
		return fmt.Errorf("connect to db: %w", err)
	}
	defer s.Close()

	events, err := s.GetAllEvents(ctx, workflowID, runID)
	if err != nil {
		return fmt.Errorf("get events: %w", err)
	}

	data, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal events: %w", err)
	}

	if err := os.WriteFile(output, data, 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	fmt.Printf("Exported %d events to %s\n", len(events), output)
	return nil
}

func runMigrations(dbConn, migrationsDir string) error {
	fmt.Println("Running database migrations...")
	fmt.Printf("Migrations directory: %s\n", migrationsDir)
	return nil
}
