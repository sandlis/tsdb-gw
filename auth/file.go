package auth

import (
	"flag"
	"path"
	"strconv"
	"strings"

	ini "github.com/glacjay/goini"
	"github.com/raintank/worldping-api/pkg/log"
)

/*
Reads an ini file containing a section for each auth Key.  Each section contains
the user details associated with that key.

example:
------------------
[org1]
orgId = 1
isAdmin = true

[org2]
orgId = 23
isAdmin = false
instances = 1,2,4
password = wpirgn123
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

	conf := ini.MustLoad(filePath)

	orgs := conf.GetSections()

	for _, org := range orgs {
		if org == "" {
			continue
		}
		password, ok := conf.GetString(org, "password")
		if !ok {
			log.Error(3, "auth.file: no password defined for org %s", org)
			continue
		}

		orgID, ok := conf.GetInt(org, "orgid")
		if !ok {
			var err error
			orgID, err = strconv.Atoi(org)
			if err != nil {
				log.Error(3, "auth.file: no orgID defined for org %s", org)
			}
			log.Debug("orgID not explicitly defined, using section header %v", org)
			continue
		}

		isAdmin, _ := conf.GetBool(org, "isadmin")
		a.keys[password] = &User{
			ID:      orgID,
			IsAdmin: isAdmin,
		}

		instances, _ := conf.GetString(org, "instances")
		if !ok {
			continue
		}
		for _, i := range strings.Split(instances, ",") {
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
