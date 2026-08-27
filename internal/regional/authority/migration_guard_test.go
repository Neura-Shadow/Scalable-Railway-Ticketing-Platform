package authority_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// migratedWriteFiles is deliberately explicit: adding a production write
// path to the regional-authority migration also adds it to this permanent
// regression gate. Read-only transactions are not swept up accidentally.
var migratedWriteFiles = []string{
	"internal/payment/ledger/postgres/store.go",
	"internal/payment/postgres/store.go",
	"internal/payment/settlement/postgres/import_store.go",
	"internal/payment/settlement/postgres/review_store.go",
	"internal/payment/settlement/postgres/detection_store.go",
	"internal/payment/webhook/postgres/repository.go",
}

func TestMigratedWritePathsCannotBeginTransactionsOutsideRegionalAuthority(t *testing.T) {
	t.Parallel()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve migration guard location")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", ".."))

	for _, relativePath := range migratedWriteFiles {
		relativePath := relativePath
		t.Run(relativePath, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(repositoryRoot, filepath.FromSlash(relativePath))
			set := token.NewFileSet()
			file, err := parser.ParseFile(set, path, nil, 0)
			if err != nil {
				t.Fatalf("parse migrated write path: %v", err)
			}

			ast.Inspect(file, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if ok && selector.Sel.Name == "BeginTx" {
					position := set.Position(selector.Pos())
					t.Errorf("direct BeginTx bypass at %s:%d; use the regional authority writer", relativePath, position.Line)
				}
				return true
			})
		})
	}
}

func TestProductionCompositionCannotOpenUnboundPostgresPools(t *testing.T) {
	t.Parallel()
	repositoryRoot := repositoryRoot(t)
	allowedImplementations := map[string]bool{
		"internal/platform/postgresx/pool.go":    true,
		"internal/sharding/physical/pgx_pool.go": true,
	}
	err := filepath.WalkDir(repositoryRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == "vendor") {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		relativePath, err := filepath.Rel(repositoryRoot, path)
		if err != nil {
			return err
		}
		relativePath = filepath.ToSlash(relativePath)
		if allowedImplementations[relativePath] {
			return nil
		}
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, nil, 0)
		if err != nil {
			return err
		}
		var directConstructors []token.Position
		regionalBinding := false
		poolAliases := importedAliases(file, "github.com/jackc/pgx/v5/pgxpool", "pgxpool")
		postgresxAliases := importedAliases(file,
			"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx", "postgresx")
		physicalAliases := importedAliases(file,
			"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical", "physical")
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			identifier, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			if postgresxAliases[identifier.Name] &&
				(selector.Sel.Name == "NewRegionalBoundedPool" || selector.Sel.Name == "ApplyRegionalSession") ||
				physicalAliases[identifier.Name] && selector.Sel.Name == "RegionalPGXPoolFactory" {
				regionalBinding = true
			}
			if poolAliases[identifier.Name] &&
				(selector.Sel.Name == "New" || selector.Sel.Name == "NewWithConfig") ||
				postgresxAliases[identifier.Name] && selector.Sel.Name == "NewBoundedPool" ||
				physicalAliases[identifier.Name] && selector.Sel.Name == "OpenPGXPool" {
				directConstructors = append(directConstructors, set.Position(selector.Pos()))
			}
			return true
		})
		if len(directConstructors) > 0 && !regionalBinding {
			for _, position := range directConstructors {
				t.Errorf("unbound PostgreSQL pool at %s:%d; install a validated regional session before constructing it",
					relativePath, position.Line)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk production composition roots: %v", err)
	}
}

func importedAliases(file *ast.File, importPath, defaultAlias string) map[string]bool {
	aliases := make(map[string]bool, 1)
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil || path != importPath {
			continue
		}
		alias := defaultAlias
		if spec.Name != nil {
			alias = spec.Name.Name
		}
		if alias != "_" && alias != "." {
			aliases[alias] = true
		}
	}
	return aliases
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve migration guard location")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", ".."))
}
