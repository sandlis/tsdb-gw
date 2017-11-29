package carbon

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/raintank/metrictank/conf"
	"gopkg.in/raintank/schema.v1"
)

func getSchemas(file string) (*conf.Schemas, error) {
	schemas, err := conf.ReadSchemas(file)
	if err != nil {
		return nil, err
	}
	return &schemas, nil
}

// parseMetric parses a buffer into a MetricData message, using the schemas to deduce the interval of the data.
// The given orgId will be applied to the MetricData
func parseMetric(buf []byte, schemas *conf.Schemas, orgId int) (*schema.MetricData, error) {
	errFmt3Fields := "%q: need 3 fields"
	errFmt := "%q: %s"

	msg := strings.TrimSpace(string(buf))

	elements := strings.Fields(msg)
	if len(elements) != 3 {
		return nil, fmt.Errorf(errFmt3Fields, msg)
	}

	metric := strings.Split(elements[0], ";")
	name := metric[0]

	var tags []string

	if len(metric) > 1 {
		for _, v := range metric[1:] {
			tagset := strings.Split(v, "=")
			if len(tagset) != 2 {
				return nil, fmt.Errorf(errFmt, msg, "unable to parse tag, more than one '=' character")
			}
			if tagset[0] == "" {
				return nil, fmt.Errorf(errFmt, msg, "unable to parse tag, no key")
			}
			if tagset[1] == "" {
				return nil, fmt.Errorf(errFmt, msg, "unable to parse tag, no value")
			}
			tags = append(tags, v)
		}
	}

	val, err := strconv.ParseFloat(elements[1], 64)
	if err != nil {
		return nil, fmt.Errorf(errFmt, msg, err)
	}

	timestamp, err := strconv.ParseUint(elements[2], 10, 32)
	if err != nil {
		return nil, fmt.Errorf(errFmt, msg, err)
	}

	_, s := schemas.Match(name, 0)
	md := metricPool.Get()
	*md = schema.MetricData{
		Name:     name,
		Metric:   name,
		Interval: s.Retentions[0].SecondsPerPoint,
		Value:    val,
		Unit:     "unknown",
		Time:     int64(timestamp),
		Mtype:    "gauge",
		Tags:     tags,
		OrgId:    orgId,
	}
	md.SetId()

	return md, nil
}
