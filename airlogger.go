package airlogger

import (
	"fmt"
	"strings"
)

func ParseArgs(args []string) (string, string) {
	arg := args[1]
	dagId := strings.ToUpper(arg)
	filename := parseFilename(arg)
	return dagId, filename
}

func parseFilename(arg string) string {
	filename := strings.ToLower(arg)
	filename = strings.ReplaceAll(filename, "_", "-")
	filename = fmt.Sprintf("%s.py", filename)
	return filename
}
