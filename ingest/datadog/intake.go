package datadog

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/raintank/tsdb-gw/api/models"
	"github.com/raintank/tsdb-gw/persister/persist"
	log "github.com/sirupsen/logrus"
	schema "gopkg.in/raintank/schema.v1"
)

func DataDogIntake(ctx *models.Context) {
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
	info.OrgID = ctx.ID
	err = json.Unmarshal(data, &info)

	if err != nil {
		ctx.JSON(400, fmt.Sprintf("unable to unmarshal request, reason: %v", err))
		return
	}

	if info.Gohai != "" {
		payload, err := json.Marshal(info)
		if err != nil {
			log.Errorf("failed to persist datadog info. %s", err)
			ctx.JSON(500, err)
			return
		}
		err = persist.Persist(payload)
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
	AgentVersion     string `json:"agentVersion"`
	UUID             string `json:"uuid"`
	OS               string `json:"os"`
	InternalHostname string `json:"internalHostname"`
	Python           string `json:"python"`
	SystemStats      struct {
		CPUCores  int      `json:"cpuCores"`
		Machine   string   `json:"machine"`
		Platform  string   `json:"platform"`
		Processor string   `json:"processor"`
		PythonV   string   `json:"pythonV"`
		MacV      []string `json:"macV"`
		NixV      []string `json:"nixV"`
		FbsdV     []string `json:"fbsdV"`
		WinV      []string `json:"winV"`
	} `json:"systemStats"`
	Meta struct {
		SocketHostname string   `json:"socket-hostname"`
		Timezones      []string `json:"timezones"`
		SocketFqdn     string   `json:"socket-fqdn"`
		Hostname       string   `json:"hostname"`
		EC2Hostname    string   `json:"ec2-hostname"`
		InstanceID     string   `json:"instance-id"`
		HostAlias      []string `json:"host_aliases"`
	} `json:"meta"`
	HostTags map[string]interface{} `json:"host-tags"`
	Tags     struct {
		System              []string `json:"system,omitempty"`
		GoogleCloudPlatform []string `json:"google cloud platform,omitempty"`
	}
	Gohai string `json:"gohai"`
	OrgID int    `json:"org-id"`
}

func (i *DataDogIntakePayload) GeneratePersistentMetrics() []*schema.MetricData {
	metrics := []*schema.MetricData{}
	tags := []string{
		"agentVersion=" + serializeTag(i.AgentVersion),
		"host=" + serializeTag(i.Meta.Hostname),
		"hostname=" + serializeTag(i.InternalHostname),
		"machine=" + serializeTag(i.SystemStats.Machine),
		"os=" + serializeTag(i.OS),
		"processor=" + serializeTag(i.SystemStats.Processor),
		"socket_fqdn=" + serializeTag(i.Meta.SocketFqdn),
		"socket_hostname=" + serializeTag(i.Meta.SocketHostname),
		"instanceID=" + serializeTag(i.Meta.InstanceID),
	}

	for _, t := range i.Tags.System {
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

	for _, t := range i.Tags.GoogleCloudPlatform {
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

	metrics = append(metrics, &schema.MetricData{
		Name:  "system_info",
		Tags:  tags,
		Value: 1,
		OrgId: i.OrgID,
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
				"hostname=" + serializeTag(i.InternalHostname),
				"kernel_name=" + serializeTag(g.Platform.KernelName),
				"kernel_release=" + serializeTag(g.Platform.KernelRelease),
				"kernel_version=" + serializeTag(g.Platform.KernelVersion),
				"machine=" + serializeTag(g.Platform.Machine),
				"os=" + serializeTag(g.Platform.Os),
				"processor=" + serializeTag(g.Platform.Processor),
				"pythonV=" + serializeTag(g.Platform.PythonV),
			},
			Value: 1,
			OrgId: i.OrgID,
		},
		&schema.MetricData{
			Name: "system_network_info",
			Tags: []string{
				"hostname=" + i.InternalHostname,
				"ipaddress=" + g.Network.Ipaddress,
				"ipaddressv6=" + g.Network.Ipaddressv6,
				"macaddress=" + g.Network.Macaddress,
			},
			Value: 1,
			OrgId: i.OrgID,
		},
	)

	for _, fs := range g.Filesystem {
		metrics = append(metrics, &schema.MetricData{
			Name: "system_filesystem_info",
			Tags: []string{
				"hostname=" + serializeTag(i.InternalHostname),
				"name=" + serializeTag(fs.Name),
				"mountpoint=" + serializeTag(fs.MountedOn),
			},
			Value: 1,
			OrgId: i.OrgID,
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
		Name      string `json:"name"`
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
