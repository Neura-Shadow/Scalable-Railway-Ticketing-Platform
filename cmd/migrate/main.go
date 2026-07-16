package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/safeerror"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("migrate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("path", "migrations", "migration directory")
	databaseFlag := flags.String("database", "", "PostgreSQL connection URL (defaults to DATABASE_URL)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	databaseURL := strings.TrimSpace(*databaseFlag)
	if databaseURL == "" {
		databaseURL = strings.TrimSpace(os.Getenv("DATABASE_URL"))
	}
	if databaseURL == "" || flags.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: migrate -path migrations [-database DATABASE_URL] {up|down|version}")
		return 2
	}
	action := flags.Arg(0)
	if action != "up" && action != "down" && action != "version" {
		fmt.Fprintln(stderr, "action must be up, down, or version")
		return 2
	}

	absolutePath, err := filepath.Abs(*path)
	if err != nil {
		fmt.Fprintln(stderr, "resolve migration path failed")
		return 1
	}
	sourceURL := "file://" + filepath.ToSlash(absolutePath)
	runner, err := migrate.New(sourceURL, databaseURL)
	if err != nil {
		fmt.Fprintln(stderr, safeerror.Database(safeerror.MigrationConnectionFailed, err))
		return 1
	}

	switch action {
	case "up":
		err = runner.Up()
	case "down":
		err = runner.Steps(-1)
	case "version":
		var version uint
		var dirty bool
		version, dirty, err = runner.Version()
		if err == nil {
			fmt.Fprintf(stdout, "version=%d dirty=%t\n", version, dirty)
		}
	}

	if errors.Is(err, migrate.ErrNoChange) {
		fmt.Fprintln(stdout, "no change")
		err = nil
	}
	if err != nil {
		fmt.Fprintln(stderr, safeerror.Database(safeerror.MigrationOperationFailed, err))
	}
	sourceErr, databaseErr := runner.Close()
	if sourceErr != nil || databaseErr != nil {
		fmt.Fprintln(stderr, safeerror.Database(safeerror.MigrationCloseFailed, errors.Join(sourceErr, databaseErr)))
		return 1
	}
	if err != nil {
		return 1
	}
	return 0
}
