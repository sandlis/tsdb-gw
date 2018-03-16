package opentsdb

type Metric struct {
	Metric    string            `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
}

type PutRequest []Metric

func (m Metric) FormatTags() []string {
	tagArray := make([]string, 0)
	for t, v := range m.Tags {
		tagArray = append(tagArray, t+"="+v)
	}
	return tagArray
}
