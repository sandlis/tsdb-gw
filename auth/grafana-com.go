package auth

import (
	"github.com/raintank/tsdb-gw/auth/gAuth"
)

type GrafanaComAuth struct {
}

func NewGrafanaComAuth() *GrafanaComAuth {
	return &GrafanaComAuth{}
}

func (a *GrafanaComAuth) Auth(userKey string) (*User, error) {
	u, err := gAuth.Auth(AdminKey, userKey)
	if err != nil {
		if err == gAuth.ErrInvalidApiKey {
			return nil, ErrInvalidKey
		}
		if err == gAuth.ErrInvalidOrgId {
			return nil, ErrInvalidOrgId
		}
		return nil, err
	}
	return &User{
		OrgId:   int(u.OrgId),
		IsAdmin: u.IsAdmin,
	}, nil
}

func (a *GrafanaComAuth) InstanceAuth(userKey string, instanceID string) (*User, error) {
	u, err := gAuth.Auth(AdminKey, userKey)
	if err != nil {
		if err == gAuth.ErrInvalidApiKey {
			return nil, ErrInvalidKey
		}
		if err == gAuth.ErrInvalidOrgId {
			return nil, ErrInvalidOrgId
		}
		return nil, err
	}
	err = u.CheckInstance(instanceID)
	if err != nil {
		return nil, err
	}
	return &User{
		OrgId:   int(u.OrgId),
		IsAdmin: u.IsAdmin,
	}, nil
}
