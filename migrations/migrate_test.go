package migrations

import (
	"bytes"
	"io/fs"
	"testing"
)

// The checksum must be the same in a Windows working tree and a clean Linux
// checkout. .gitattributes fixes SQL checkout line endings; catch accidental CR
// bytes before applying an embedded migration to another database.
func TestEmbeddedSQLHasCanonicalLineEndings(t *testing.T) {
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		b, err := files.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.ContainsRune(b, '\r') || !bytes.HasSuffix(b, []byte("\n")) {
			t.Fatalf("%s must use LF line endings", name)
		}
	}
}
