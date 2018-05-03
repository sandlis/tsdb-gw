package datadog

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/raintank/tsdb-gw/api"
	"github.com/raintank/tsdb-gw/persister/persist"
	log "github.com/sirupsen/logrus"
	schema "gopkg.in/raintank/schema.v1"
)

func DataDogIntake(ctx *api.Context) {
	if ctx.Req.Request.Body == nil {
		ctx.JSON(400, "no data included in request.")
		return
	}
	defer ctx.Req.Request.Body.Close()

	data, err := decodeJSON(ctx.Req.Request.Body, ctx.Req.Request.Header.Get("Content-Encoding") == "deflate")
	if err != nil {
		ctx.JSON(400, fmt.Sprintf("unable to decode request, reason: %v", err))
		return
	}

	var info DataDogIntakePayload
	err = json.Unmarshal(data, &info)
	if err != nil {
		ctx.JSON(400, fmt.Sprintf("unable to unmarshal request, reason: %v", err))
		return
	}

	if info.Gohai != "" {
		err = persist.Persist(info.GeneratePersistentMetrics(ctx.ID))
		if err != nil {
			log.Errorf("failed to persist datadog info. %s", err)
			ctx.JSON(500, err)
			return
		}
	}

	ctx.JSON(200, "ok")
	return
}

type DataDogIntakePayload struct {
	RAW          []byte
	AgentVersion string `json:"agentVersion"`
	OS           string `json:"os"`
	SystemStats  struct {
		Machine   string `json:"machine"`
		Processor string `json:"processor"`
	} `json:"systemStats"`
	Meta struct {
		SocketHostname string `json:"socket-hostname"`
		SocketFqdn     string `json:"socket-fqdn"`
		Hostname       string `json:"hostname"`
	} `json:"meta"`
	Tags struct {
		System              []string `json:"system,omitempty"`
		GoogleCloudPlatform []string `json:"google cloud platform,omitempty"`
	}
	Gohai string `json:"gohai"`
}

func (i *DataDogIntakePayload) GeneratePersistentMetrics(orgID int) []*schema.MetricData {
	metrics := []*schema.MetricData{}
	systemTags := []string{
		"agentVersion=" + i.AgentVersion,
		"hostname=" + i.Meta.Hostname,
		"machine=" + i.SystemStats.Machine,
		"os=" + i.OS,
		"processor=" + i.SystemStats.Processor,
		"socket_fqdn=" + i.Meta.SocketFqdn,
		"socket_hostname=" + i.Meta.SocketHostname,
	}

	metrics = append(metrics, &schema.MetricData{
		Name:  "system_info",
		Tags:  systemTags,
		Value: 1,
		OrgId: orgID,
	})

	gohaiJSON := strings.Replace(i.Gohai, `\"`, `"`, -1)
	g := Gohai{}

	err := json.Unmarshal([]byte(gohaiJSON), &g)
	if err != nil {
		log.Errorf("unable to decode Gohai payload: %v", err)
		return metrics
	}

	metrics = append(metrics,
		&schema.MetricData{
			Name: "system_platform_info",
			Tags: []string{
				"GOOARCH=" + serializeTag(g.Platform.GOOARCH),
				"GOOS=" + serializeTag(g.Platform.GOOS),
				"GoV=" + serializeTag(g.Platform.GoV),
				"HardwarePlatform=" + serializeTag(g.Platform.HardwarePlatform),
				"hostname=" + serializeTag(g.Platform.Hostname),
				"kernel_name=" + serializeTag(g.Platform.KernelName),
				"kernel_release=" + serializeTag(g.Platform.KernelRelease),
				"kernel_version=" + serializeTag(g.Platform.KernelVersion),
				"machine=" + serializeTag(g.Platform.Machine),
				"os=" + serializeTag(g.Platform.Os),
				"processor=" + serializeTag(g.Platform.Processor),
				"pythonV=" + serializeTag(g.Platform.PythonV),
			},
			Value: 1,
		},
		&schema.MetricData{
			Name: "system_network_info",
			Tags: []string{
				"hostname=" + g.Platform.Hostname,
				"ipaddress=" + g.Network.Ipaddress,
				"ipaddressv6=" + g.Network.Ipaddressv6,
				"macaddress=" + g.Network.Macaddress,
			},
			Value: 1,
		},
	)

	for _, fs := range g.Filesystem {
		metrics = append(metrics, &schema.MetricData{
			Name: "system_filesystem_info",
			Tags: []string{
				"hostname=" + serializeTag(g.Platform.Hostname),
				"device=" + serializeTag(fs.Device),
				"mountpoint=" + serializeTag(fs.MountedOn),
			},
			Value: 1,
		})
	}

	return metrics
}

type Gohai struct {
	CPU struct {
		CacheSize            string `json:"cache_size"`
		CPUCores             string `json:"cpu_cores"`
		CPULogicalProcessors string `json:"cpu_logical_processors"`
		Family               string `json:"family"`
		Mhz                  string `json:"mhz"`
		Model                string `json:"model"`
		ModelName            string `json:"model_name"`
		Stepping             string `json:"stepping"`
		VendorID             string `json:"vendor_id"`
	} `json:"cpu"`
	Filesystem []struct {
		MountedOn string `json:"mounted_on"`
		Device    string `json:"device"`
	}
	Network struct {
		Ipaddress   string `json:"ipaddress"`
		Ipaddressv6 string `json:"ipaddressv6"`
		Macaddress  string `json:"macaddress"`
	} `json:"network"`
	Platform struct {
		GOOARCH          string `json:"GOOARCH"`
		GOOS             string `json:"GOOS"`
		GoV              string `json:"goV"`
		HardwarePlatform string `json:"hardware_platform"`
		Hostname         string `json:"hostname"`
		KernelName       string `json:"kernel_name"`
		KernelRelease    string `json:"kernel_release"`
		KernelVersion    string `json:"kernel_version"`
		Machine          string `json:"machine"`
		Os               string `json:"os"`
		Processor        string `json:"processor"`
		PythonV          string `json:"pythonV"`
	} `json:"platform"`
}
