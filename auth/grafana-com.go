package auth

import (
	"github.com/raintank/tsdb-gw/auth/gcom"
	"strconv"
)

type GrafanaComAuth struct {
}

func NewGrafanaComAuth() *GrafanaComAuth {
	return &GrafanaComAuth{}
}

func (a *GrafanaComAuth) Auth(username, password string) (*User, error) {
	u, err := gcom.Auth(AdminKey, password)
	if err != nil {
		if err == gcom.ErrInvalidApiKey {
			return nil, ErrInvalidCredentials
		}
		if err == gcom.ErrInvalidOrgId {
			return nil, ErrInvalidOrgId
		}
		return nil, err
	}

	ctxUser := &User{
		ID:      int(u.OrgId),
		IsAdmin: u.IsAdmin,
	}

	if username != "api_key" {
		// ensure that the instanceId is an integer.
		instanceID, err := strconv.ParseInt(username, 10, 64)
		if err != nil {
			return nil, ErrInvalidCredentials
		}
		err = u.CheckInstance(username)
		if err != nil {
			return nil, ErrInvalidCredentials
		}
		ctxUser.ID = int(instanceID)
	}
	return ctxUser, nil
}
