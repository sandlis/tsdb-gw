package auth

import (
	"github.com/raintank/tsdb-gw/auth/gcom"
)

type GrafanaInstanceAuth struct {
}

func NewGrafanaInstanceAuth() *GrafanaInstanceAuth {
	return &GrafanaInstanceAuth{}
}

func (a *GrafanaInstanceAuth) Auth(username string, password string) (*User, error) {
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
	err = u.CheckInstance(username)
	if err != nil {
		return nil, err
	}
	return &User{
		ID:      int(u.OrgId),
		IsAdmin: u.IsAdmin,
	}, nil
}
