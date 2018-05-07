package datadog

import (
	"compress/zlib"
	"io"
	"io/ioutil"
	"sort"
	"strings"
)

func createTagSet(host string, device string, ctags []string) []string {
	tags := []string{}
	if device != "" {
		tags = append(tags, "device="+device)
	}
	tags = append(tags, "host="+host)
	for _, t := range ctags {
		tSplit := strings.SplitN(t, ":", 2)
		if len(tSplit) == 0 {
			continue
		}
		if len(tSplit) == 1 {
			tags = append(tags, tSplit[0])
			continue
		}
		if tSplit[1] == "" {
			tags = append(tags, tSplit[0])
			continue
		}
		tags = append(tags, tSplit[0]+"="+tSplit[1])
	}
	sort.Strings(tags)
	return tags
}

func decodeJSON(body io.ReadCloser, encoded bool) ([]byte, error) {
	var err error
	if encoded {
		body, err = zlib.NewReader(body)
		if err != nil {
			return nil, err
		}
	}
	return ioutil.ReadAll(body)

}
