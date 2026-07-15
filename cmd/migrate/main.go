package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func main() {
	path := flag.String("path", "migrations", "migration directory")
	databaseURL := flag.String("database", "", "PostgreSQL connection URL")
	flag.Parse()
	if *databaseURL == "" || flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: migrate -path migrations -database DATABASE_URL {up|down|version}")
		os.Exit(2)
	}

	absolutePath, err := filepath.Abs(*path)
	if err != nil {
		log.Fatal("resolve migration path: ", err)
	}
	sourceURL := "file://" + filepath.ToSlash(absolutePath)
	runner, err := migrate.New(sourceURL, *databaseURL)
	if err != nil {
		log.Fatal("open migrations: ", err)
	}
	defer func() {
		sourceErr, databaseErr := runner.Close()
		if sourceErr != nil || databaseErr != nil {
			log.Printf("close migrations: source=%v database=%v", sourceErr, databaseErr)
		}
	}()

	switch flag.Arg(0) {
	case "up":
		err = runner.Up()
	case "down":
		err = runner.Steps(-1)
	case "version":
		var version uint
		var dirty bool
		version, dirty, err = runner.Version()
		if err == nil {
			fmt.Printf("version=%d dirty=%t\n", version, dirty)
		}
	default:
		fmt.Fprintln(os.Stderr, "action must be up, down, or version")
		os.Exit(2)
	}

	if errors.Is(err, migrate.ErrNoChange) {
		fmt.Println("no change")
		return
	}
	if err != nil {
		log.Fatal(err)
	}
}
