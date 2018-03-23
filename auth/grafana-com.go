package auth

import (
	"github.com/raintank/tsdb-gw/auth/gcom"
)

type GrafanaComAuth struct {
}

func NewGrafanaComAuth() *GrafanaComAuth {
	return &GrafanaComAuth{}
}

func (a *GrafanaComAuth) Auth(instanceID, password string) (*User, error) {
	u, err := gcom.Auth(AdminKey, password)
	if err != nil {
		if err == gcom.ErrInvalidApiKey {
			return nil, ErrInvalidKey
		}
		if err == gcom.ErrInvalidOrgId {
			return nil, ErrInvalidOrgId
		}
		return nil, err
	}
	if instanceID != "api_key" {
		err = u.CheckInstance(instanceID)
		if err != nil {
			return nil, err
		}
	}
	return &User{
		ID:      int(u.OrgId),
		IsAdmin: u.IsAdmin,
	}, nil
}
