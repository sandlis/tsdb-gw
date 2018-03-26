package auth

import (
	"github.com/raintank/tsdb-gw/auth/gcom"
	"strconv"
)

type GrafanaComInstanceAuth struct {
}

func NewGrafanaComInstanceAuth() *GrafanaComInstanceAuth {
	return &GrafanaComInstanceAuth{}
}

func (a *GrafanaComInstanceAuth) Auth(username, password string) (*User, error) {
	// ensure that the username is an integer.
	instanceID, err := strconv.ParseInt(username, 10, 64)
	if err != nil {
		return nil, ErrInvalidCredentials
	}
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

	err = u.CheckInstance(username)
	if err != nil {
		return nil, ErrInvalidCredentials
	}

	return &User{
		ID:      int(instanceID),
		IsAdmin: u.IsAdmin,
	}, nil
}
