package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
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
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	databaseURL := strings.TrimSpace(*databaseFlag)
	if databaseURL == "" {
		databaseURL = strings.TrimSpace(os.Getenv("DATABASE_URL"))
	}
	if databaseURL == "" || flags.NArg() < 1 {
		fmt.Fprintln(stderr, "usage: migrate -path migrations [-database DATABASE_URL] {up|down|version|up-to VERSION}")
		return 2
	}
	action := flags.Arg(0)
	var targetVersion uint64
	if action == "up-to" {
		if flags.NArg() != 2 {
			fmt.Fprintln(stderr, "up-to requires exactly one positive target version")
			return 2
		}
		parsed, parseErr := strconv.ParseUint(flags.Arg(1), 10, 32)
		if parseErr != nil || parsed == 0 {
			fmt.Fprintln(stderr, "up-to target must be a positive 32-bit version")
			return 2
		}
		targetVersion = parsed
	} else if (action != "up" && action != "down" && action != "version") || flags.NArg() != 1 {
		fmt.Fprintln(stderr, "action must be up, down, version, or up-to VERSION")
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
	case "up-to":
		var current uint
		var dirty bool
		current, dirty, err = runner.Version()
		if errors.Is(err, migrate.ErrNilVersion) {
			current, dirty, err = 0, false, nil
		}
		if err == nil && dirty {
			err = errors.New("migration state is dirty")
		}
		if err == nil && uint64(current) > targetVersion {
			err = errors.New("up-to target precedes current version")
		}
		if err == nil && uint64(current) == targetVersion {
			err = migrate.ErrNoChange
		}
		if err == nil {
			err = runner.Migrate(uint(targetVersion))
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
