package gcom

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type int64SliceFlag []int64

func (i *int64SliceFlag) Set(value string) error {
	for _, split := range strings.Split(value, ",") {
		if split == "" {
			continue
		}
		parsed, err := strconv.Atoi(split)
		if err != nil {
			return err
		}
		*i = append(*i, int64(parsed))
	}
	return nil
}

func (i *int64SliceFlag) String() string {
	return strings.Trim(strings.Replace(fmt.Sprint(*i), " ", ", ", -1), "[]")
}

var (
	validTTL      = time.Minute * 5
	invalidTTL    = time.Second * 30
	authEndpoint  = "https://grafana.com"
	validOrgIds   = int64SliceFlag{}
	cache         *AuthCache
	instanceCache *InstanceAuthCache

	// global HTTP client.  By sharing the client we can take
	// advantage of keepalives and re-use connections instead
	// of establishing a new tcp connection for every request.
	client = &http.Client{
		Timeout: time.Second * 2,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          10,
			IdleConnTimeout:       300 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
)

type AuthCache struct {
	sync.RWMutex
	items map[string]CacheItem
}

type CacheItem struct {
	User       *SignedInUser
	ExpireTime time.Time
}

func (a *AuthCache) Get(key string) (*SignedInUser, bool) {
	a.RLock()
	defer a.RUnlock()
	if c, ok := a.items[key]; ok {
		return c.User, c.ExpireTime.After(time.Now())
	}
	return nil, false
}

func (a *AuthCache) Set(key string, u *SignedInUser, ttl time.Duration) {
	a.Lock()
	a.items[key] = CacheItem{
		User:       u,
		ExpireTime: time.Now().Add(ttl),
	}
	a.Unlock()
}

func (a *AuthCache) Clear() {
	a.Lock()
	a.items = make(map[string]CacheItem)
	a.Unlock()
}

type InstanceAuthCache struct {
	sync.RWMutex
	items map[string]InstanceCacheItem
}

type InstanceCacheItem struct {
	valid      bool
	ExpireTime time.Time
}

func (a *InstanceAuthCache) Get(key string) (bool, bool) {
	a.RLock()
	defer a.RUnlock()
	if c, ok := a.items[key]; ok {
		return c.valid, c.ExpireTime.After(time.Now())
	}
	return false, false
}

func (a *InstanceAuthCache) Set(key string, valid bool, ttl time.Duration) {
	a.Lock()
	a.items[key] = InstanceCacheItem{
		valid:      valid,
		ExpireTime: time.Now().Add(ttl),
	}
	a.Unlock()
}

func (a *InstanceAuthCache) Clear() {
	a.Lock()
	a.items = make(map[string]InstanceCacheItem)
	a.Unlock()
}

func init() {
	flag.StringVar(&authEndpoint, "auth-endpoint", authEndpoint, "Endpoint to authenticate users on")
	flag.DurationVar(&validTTL, "auth-valid-ttl", validTTL, "how long valid auth responses should be cached")
	flag.DurationVar(&invalidTTL, "auth-invalid-ttl", invalidTTL, "how long invalid auth responses should be cached")
	flag.Var(&validOrgIds, "auth-valid-org-id", "restrict authentication to the listed orgId (comma separated list)")
	cache = &AuthCache{items: make(map[string]CacheItem)}
	instanceCache = &InstanceAuthCache{items: make(map[string]InstanceCacheItem)}
}

func Auth(adminKey, keyString string) (*SignedInUser, error) {
	if keyString == adminKey {
		return &SignedInUser{
			Role:    ROLE_ADMIN,
			OrgId:   1,
			OrgName: "Admin",
			OrgSlug: "admin",
			IsAdmin: true,
			key:     keyString,
		}, nil
	}

	user, cached := cache.Get(keyString)
	if cached {
		if user != nil {
			log.Debug("Auth: valid key cached")
			return user, nil
		}
		log.Debug("Auth: invalid key cached")
		return nil, ErrInvalidApiKey
	}

	payload := url.Values{}
	payload.Add("token", keyString)

	res, err := client.PostForm(fmt.Sprintf("%s/api/api-keys/check", authEndpoint), payload)
	if err != nil {
		// if we have an expired cached entry for the user, reset the cache expiration and return that
		// this allows the service to remain available if grafana.net is unreachable
		if user != nil {
			log.Debug("Auth: re-caching validKey response for %d seconds", validTTL/time.Second)
			cache.Set(keyString, user, validTTL)
			return user, nil
		}

		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()

	if res.StatusCode >= 500 {
		// if we have an expired cached entry for the user, reset the cache expiration and return that
		// this allows the service to remain available if grafana.net is unreachable
		if user != nil {
			log.Debug("Auth: re-caching validKey response for %d seconds", validTTL/time.Second)
			cache.Set(keyString, user, validTTL)
			return user, nil
		}

		return nil, err
	}

	if res.StatusCode != 200 {
		// add the invalid key to the cache
		log.Debug("Auth: Caching invalidKey response for %d seconds", invalidTTL/time.Second)
		cache.Set(keyString, nil, invalidTTL)

		return nil, ErrInvalidApiKey
	}

	user = &SignedInUser{key: keyString}
	err = json.Unmarshal(body, user)
	if err != nil {
		return nil, err
	}

	valid := false
	// keeping it backwards compatible
	if len(validOrgIds) == 0 {
		valid = true
	} else {
		for _, id := range validOrgIds {
			if user.OrgId == id {
				valid = true
				break
			}
		}
	}

	if !valid {
		log.Debug("Auth: orgID is not listed in auth-valid-org-id setting.")
		return nil, ErrInvalidOrgId
	}

	// add the user to the cache.
	log.Debug("Auth: Caching validKey response for %d seconds", validTTL/time.Second)
	cache.Set(keyString, user, validTTL)
	return user, nil
}

// validate that the signedInUser has a hosted-metrics instance with the
// passed instanceID.  It is assumed that the instanceID has already been
// confirmed to be an integer.
func (u *SignedInUser) CheckInstance(instanceID string) error {
	instanceSlug := u.OrgSlug + "-" + instanceID

	// check the cache
	log.Debug("Auth: Checking cache for instance")
	valid, cached := instanceCache.Get(instanceSlug)
	if cached {
		if valid {
			log.Debug("Auth: valid instance key cached")
			return nil
		}

		log.Debug("Auth: invalid instance key cached")
		return ErrInvalidInstanceID
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/hosted-metrics/%s", authEndpoint, instanceID), nil)
	if err != nil {
		// if we have an expired cached entry for the user, reset the cache expiration and return that
		// this allows the service to remain available if grafana.net is unreachable
		if valid {
			log.Debug("Auth: re-caching valid instance response for %d seconds", validTTL/time.Second)
			instanceCache.Set(instanceSlug, true, validTTL)
			return nil
		}

		return err
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", u.key))

	res, err := client.Do(req)
	if err != nil {
		// if we have an expired cached entry for the user, reset the cache expiration and return that
		// this allows the service to remain available if grafana.net is unreachable
		if valid {
			log.Debug("Auth: re-caching valid instance response for %d seconds", validTTL/time.Second)
			instanceCache.Set(instanceSlug, true, validTTL)
			return nil
		}

		return err
	}

	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()

	log.Debug("Auth: hosted-metrics response was: %s", body)

	if res.StatusCode >= 500 {
		// if we have an expired cached entry for the instanceID, reset the cache expiration and return that
		// this allows the service to remain available if grafana.net is unreachable
		if valid {
			log.Debug("Auth: re-caching valid instance response for %d seconds", validTTL/time.Second)
			instanceCache.Set(instanceSlug, true, validTTL)
			return nil
		}

		return err
	}

	// add the invalid instance ID to the cache
	if res.StatusCode != 200 {
		log.Debug("Auth: Caching invalid instance ID response for %d seconds", invalidTTL/time.Second)
		instanceCache.Set(instanceSlug, false, invalidTTL)
		return ErrInvalidInstanceID
	}

	instance := &Instance{}
	err = json.Unmarshal(body, instance)
	if err != nil {
		return err
	}

	if strconv.Itoa(int(instance.ID)) != instanceID {
		log.Error(3, "Auth: instanceID returned from grafana.com doesnt match requested instanceID. %d != %s", instance.ID, instanceID)
		return fmt.Errorf("instance.ID returned from grafana.com doesnt match requested instanceID")
	}

	log.Debug("Auth: Caching valid instance response for %d seconds", validTTL/time.Second)
	instanceCache.Set(instanceSlug, true, validTTL)
	return nil
}
