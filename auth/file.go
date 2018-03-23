package auth

import (
	"flag"
	"path"

	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/ini.v1"
)

/*
Reads an ini file containing a section for each auth Key.  Each section contains
the user details associated with that key.

example:
------------------
[aaeipgnq]
orgId = 1
isAdmin = true

[wpirgn123]
orgId = 23
isAdmin = false
instances = 1,2,4
-------------------
*/
type FileAuth struct {
	keys        map[string]*User //map auth key to orgId
	instanceMap map[string]int
	filePath    string
}

var filePath string

func init() {
	flag.StringVar(&filePath, "auth-file-path", "/etc/raintank/tsdb-auth.ini", "path to ini file containing user details")
}

func NewFileAuth() *FileAuth {
	log.Info("loading carbon auth file from %s", filePath)
	a := &FileAuth{
		keys:        make(map[string]*User),
		instanceMap: make(map[string]int),
		filePath:    path.Clean(filePath),
	}

	conf, err := ini.Load(filePath)
	if err != nil {
		log.Fatal(4, "could not load auth file: %v", filePath)
	}

	for _, section := range conf.Sections() {
		if section.Name() == "" || section.Name() == "DEFAULT" {
			continue
		}

		orgKey, err := section.GetKey("orgId")
		if err != nil {
			log.Error(3, "auth.file: no orgID defined for org %s", section.Name())
			continue
		}

		orgID, err := orgKey.Int()
		if err != nil {
			log.Error(3, "auth.file: orgID '%v' is not a int", orgKey.String())
			continue
		}

		var isAdmin bool
		if section.Haskey("isadmin") {
			isAdminKey, err := section.GetKey("isadmin")
			if err != nil {
				log.Error(3, "auth.file: error decoding isadmin: '%v'", err)
			}
			isAdmin = isAdminKey.MustBool(false)
		}

		a.keys[section.Name()] = &User{
			ID:      orgID,
			IsAdmin: isAdmin,
		}

		if !section.Haskey("instances") {
			continue
		}

		instanceKey, err := section.GetKey("instances")
		if err != nil {
			log.Error(3, "auth.file: error decoding instances: '%v'", err)
			continue
		}
		instances := instanceKey.Strings(",")
		for _, i := range instances {
			a.instanceMap[i] = orgID
		}
	}
	if len(a.keys) == 0 {
		log.Fatal(4, "no auth credentials found in auth-file.")
	}

	return a
}

func (a *FileAuth) Auth(instanceID, password string) (*User, error) {
	if password == AdminKey {
		return AdminUser, nil
	}
	user, ok := a.keys[password]
	if !ok {
		log.Debug("key not found: %v", password)
		return nil, ErrInvalidKey
	}

	if user.IsAdmin {
		return user, nil
	}

	if instanceID != "api_key" {
		ID, ok := a.instanceMap[instanceID]
		if !ok {
			return nil, ErrInvalidInstanceID
		}
		if ID != user.ID {
			return nil, ErrPermissions
		}
	}

	return user, nil
}
