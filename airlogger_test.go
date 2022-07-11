package airlogger_test

import (
	"testing"
	"github.com/mr-joshcrane/airlogger"
)

func TestParseArgsReturnsDagIdAndFilename(t *testing.T) {
	t.Parallel()
	args := []string{"airlogger", "example_dag"}

	dagId, filename := airlogger.ParseArgs(args)
	if dagId != "EXAMPLE_DAG" {
		t.Fatalf("wanted dagId to be EXAMPLE_DAG, got  %s", dagId)
	}
	if filename != "example-dag.py" {
		t.Fatalf("wanted filename to be example-dag.py, got  %s", filename)
	}
}
