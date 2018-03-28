package auth

import (
	"errors"
	"flag"

	log "github.com/sirupsen/logrus"
)

var (
	ErrInvalidCredentials = errors.New("invalid authentication credentials")
	ErrInvalidOrgId       = errors.New("invalid orgId")
	ErrInvalidInstanceID  = errors.New("invalid instanceID")

	AdminKey  string
	AdminUser = &User{
		ID:      1,
		IsAdmin: true,
	}
)

func init() {
	flag.StringVar(&AdminKey, "admin-key", "not_very_secret_key", "Admin Secret Key")
}

type User struct {
	ID      int
	IsAdmin bool
}

// AuthPlugin is used to validate access
type AuthPlugin interface {
	// Auth returns whether a api_key is a valid and if the user has access to a certain instance
	Auth(username, password string) (*User, error)
}

func GetAuthPlugin(name string) AuthPlugin {
	switch name {
	case "grafana":
		return NewGrafanaComAuth()
	case "grafana-instance":
		return NewGrafanaComInstanceAuth()
	case "file":
		return NewFileAuth()
	default:
		log.Fatalf("invalid auth plugin specified, %s", name)
	}
	return nil
}
