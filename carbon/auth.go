package carbon

import (
	"bufio"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/raintank/worldping-api/pkg/log"
)

var (
	AuthKeys map[string]int //map auth key to orgId
)

func InitAuth(filePath string) error {
	log.Info("loading carbon auth file from %s", filePath)
	AuthKeys = make(map[string]int)
	filePath = path.Clean(filePath)

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(bufio.NewReader(file))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") || len(line) < 1 {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) < 2 {
			continue
		}

		orgId, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			log.Error(3, "failed to parse orgId from auth file. %s", err)
			continue
		}
		AuthKeys[strings.TrimSpace(parts[0])] = int(orgId)
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
