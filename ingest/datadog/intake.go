package datadog

import (
	"encoding/json"
	"fmt"

	"github.com/raintank/tsdb-gw/api/models"
	"github.com/raintank/tsdb-gw/ingest/datadog/payloads"
	"github.com/raintank/tsdb-gw/persister/persist"
	log "github.com/sirupsen/logrus"
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

	var info payloads.DataDogIntakePayload
	err = json.Unmarshal(data, &info)

	if err != nil {
		ctx.JSON(400, fmt.Sprintf("unable to unmarshal request, reason: %v", err))
		return
	}

	if info.Gohai != "" {
		payload, err := json.Marshal(payloads.PersistPayload{OrgID: ctx.ID, Hostname: info.InternalHostname, Raw: data})
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
