package auth

import (
	"errors"
	"flag"

	"github.com/raintank/worldping-api/pkg/log"
)

var (
	ErrInvalidKey        = errors.New("invalid key")
	ErrInvalidOrgId      = errors.New("invalid orgId")
	ErrInvalidInstanceID = errors.New("invalid instanceID")
	ErrPermissions       = errors.New("user does not have access to that instance")

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
	Auth(username, password string) (*User, error) // Auth returns whether a api_key is a valid and if the user has access to a certain instance
}

func GetAuthPlugin(name string) AuthPlugin {
	switch name {
	case "grafana":
		return NewGrafanaComAuth()
	case "file":
		return NewFileAuth()
	default:
		log.Fatal(4, "invalid auth plugin specified, %s", name)
	}
	return nil
}
