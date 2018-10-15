package gcom

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	. "github.com/smartystreets/goconvey/convey"
)

func TestFlags(t *testing.T) {
	Convey("When setting auth-valid-org-id to empty string", t, func() {
		validOrgIds = int64SliceFlag{}
		err := flag.Set("auth-valid-org-id", "")
		So(err, ShouldBeNil)
		So(validOrgIds, ShouldHaveLength, 0)
	})
	Convey("When setting auth-valid-org-id has no values", t, func() {
		validOrgIds = int64SliceFlag{}
		err := flag.Set("auth-valid-org-id", ", ")
		So(err, ShouldBeNil)
		So(validOrgIds, ShouldHaveLength, 0)
	})

	Convey("When setting auth-valid-org-id to invalid value", t, func() {
		validOrgIds = int64SliceFlag{}
		err := flag.Set("auth-valid-org-id", "foo")
		So(err, ShouldHaveSameTypeAs, &strconv.NumError{})
	})

	Convey("When setting auth-valid-org-id to single org", t, func() {
		validOrgIds = int64SliceFlag{}
		err := flag.Set("auth-valid-org-id", "10")
		So(err, ShouldBeNil)
		So(validOrgIds, ShouldHaveLength, 1)
		So(validOrgIds[0], ShouldEqual, 10)
	})

	Convey("When setting auth-valid-org-id to many orgs", t, func() {
		validOrgIds = int64SliceFlag{}
		err := flag.Set("auth-valid-org-id", "10,1,17")
		So(err, ShouldBeNil)
		So(validOrgIds, ShouldHaveLength, 3)
		So(validOrgIds[0], ShouldEqual, 10)
		So(validOrgIds[1], ShouldEqual, 1)
		So(validOrgIds[2], ShouldEqual, 17)
	})

	Convey("When auth-valid-org-id setting has spaces", t, func() {
		validOrgIds = int64SliceFlag{}
		err := flag.Set("auth-valid-org-id", " 10 , 1, 17")
		So(err, ShouldBeNil)
		So(validOrgIds, ShouldHaveLength, 3)
		So(validOrgIds[0], ShouldEqual, 10)
		So(validOrgIds[1], ShouldEqual, 1)
		So(validOrgIds[2], ShouldEqual, 17)
	})

	Convey("When auth-valid-org-id setting has repeated commas", t, func() {
		validOrgIds = int64SliceFlag{}
		err := flag.Set("auth-valid-org-id", ",,,10")
		So(err, ShouldBeNil)
		So(validOrgIds, ShouldHaveLength, 1)
	})
}

func TestAuth(t *testing.T) {
	mockTransport := httpmock.NewMockTransport()
	client.Transport = mockTransport
	validOrgIds = int64SliceFlag{}
	testUser := SignedInUser{
		Id:        3,
		OrgName:   "awoods Test",
		OrgSlug:   "awoodsTest",
		OrgId:     2,
		Name:      "testKey",
		Role:      ROLE_EDITOR,
		CreatedAt: time.Now(),
		key:       "foo",
	}

	Convey("When authenticating with adminKey", t, func() {
		user, err := Auth("key", "key")
		So(err, ShouldBeNil)
		So(user.Role, ShouldEqual, ROLE_ADMIN)
		So(user.OrgId, ShouldEqual, 1)
		So(user.OrgName, ShouldEqual, "Admin")
		So(user.IsAdmin, ShouldEqual, true)
		So(user.key, ShouldEqual, "key")
	})
	Convey("when authenticating with valid Key", t, func() {
		responder, err := httpmock.NewJsonResponder(200, &testUser)
		So(err, ShouldBeNil)
		mockTransport.RegisterResponder("POST", "https://grafana.com/api/api-keys/check", responder)

		user, err := Auth("key", "foo")
		So(err, ShouldBeNil)
		So(user.Role, ShouldEqual, testUser.Role)
		So(user.OrgId, ShouldEqual, testUser.OrgId)
		So(user.OrgName, ShouldEqual, testUser.OrgName)
		So(user.OrgSlug, ShouldEqual, testUser.OrgSlug)
		So(user.IsAdmin, ShouldEqual, testUser.IsAdmin)
		So(user.key, ShouldEqual, testUser.key)
		mockTransport.Reset()
	})

	Convey("When authenticating using cache", t, func() {
		cache.Set("foo", &testUser, time.Second)
		mockTransport.RegisterNoResponder(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("unexpected request made. %s %s", req.Method, req.URL.String())
			return nil, nil
		})
		user, err := Auth("key", "foo")
		So(err, ShouldBeNil)
		So(user.Role, ShouldEqual, testUser.Role)
		So(user.OrgId, ShouldEqual, testUser.OrgId)
		So(user.OrgName, ShouldEqual, testUser.OrgName)
		So(user.OrgSlug, ShouldEqual, testUser.OrgSlug)
		So(user.IsAdmin, ShouldEqual, testUser.IsAdmin)
		So(user.key, ShouldEqual, testUser.key)
		mockTransport.Reset()
	})

	Convey("When authenticating with invalid org id 1", t, func() {
		cache.Clear()
		responder, err := httpmock.NewJsonResponder(200, &testUser)
		So(err, ShouldBeNil)
		mockTransport.RegisterResponder("POST", "https://grafana.com/api/api-keys/check", responder)

		originalValidOrgIds := validOrgIds
		defer func() { validOrgIds = originalValidOrgIds }()
		validOrgIds = int64SliceFlag{1}

		user, err := Auth("key", "foo")
		So(user, ShouldBeNil)
		So(err, ShouldEqual, ErrInvalidOrgId)
		mockTransport.Reset()
	})

	Convey("When authenticating with invalid org id 2", t, func() {
		cache.Clear()
		responder, err := httpmock.NewJsonResponder(200, &testUser)
		So(err, ShouldBeNil)
		mockTransport.RegisterResponder("POST", "https://grafana.com/api/api-keys/check", responder)

		originalValidOrgIds := validOrgIds
		defer func() { validOrgIds = originalValidOrgIds }()

		validOrgIds = int64SliceFlag{3, 4, 5}
		user, err := Auth("key", "foo")
		So(user, ShouldBeNil)
		So(err, ShouldEqual, ErrInvalidOrgId)
		mockTransport.Reset()
	})

	Convey("When authenticating with explicitely valid org id", t, func() {
		cache.Clear()
		responder, err := httpmock.NewJsonResponder(200, &testUser)
		So(err, ShouldBeNil)
		mockTransport.RegisterResponder("POST", "https://grafana.com/api/api-keys/check", responder)

		originalValidOrgIds := validOrgIds
		defer func() { validOrgIds = originalValidOrgIds }()

		validOrgIds = int64SliceFlag{1, 2, 3, 4}
		user, err := Auth("key", "foo")
		So(err, ShouldBeNil)
		So(user.Role, ShouldEqual, testUser.Role)
		So(user.OrgId, ShouldEqual, testUser.OrgId)
		So(user.OrgName, ShouldEqual, testUser.OrgName)
		So(user.OrgSlug, ShouldEqual, testUser.OrgSlug)
		So(user.IsAdmin, ShouldEqual, testUser.IsAdmin)
		So(user.key, ShouldEqual, testUser.key)
		mockTransport.Reset()
	})

	Convey("When authenticating using expired cache", t, func() {
		cache.Set("bar", &testUser, 0)
		responder, err := httpmock.NewJsonResponder(200, &testUser)
		So(err, ShouldBeNil)
		mockTransport.RegisterResponder("POST", "https://grafana.com/api/api-keys/check", responder)

		// make sure cached item is expired.
		cuser, valid := cache.Get("bar")
		So(cuser, ShouldNotBeNil)
		So(valid, ShouldBeFalse)

		user, err := Auth("key", "bar")
		So(err, ShouldBeNil)
		So(user.Role, ShouldEqual, testUser.Role)
		So(user.OrgId, ShouldEqual, testUser.OrgId)
		So(user.OrgName, ShouldEqual, testUser.OrgName)
		So(user.OrgSlug, ShouldEqual, testUser.OrgSlug)
		So(user.IsAdmin, ShouldEqual, testUser.IsAdmin)
		So(user.key, ShouldEqual, "bar")

		// make sure cache is now updated.
		cuser, valid = cache.Get("bar")
		So(cuser, ShouldNotBeNil)
		So(valid, ShouldBeTrue)

		mockTransport.Reset()
	})

	Convey("When authenticating using expired cache and bad g.net response", t, func() {
		cache.Set("baz", &testUser, 0)
		responder, err := httpmock.NewJsonResponder(503, nil)
		So(err, ShouldBeNil)
		mockTransport.RegisterResponder("POST", "https://grafana.com/api/api-keys/check", responder)

		// make sure cached item is expired.
		cuser, valid := cache.Get("baz")
		So(cuser, ShouldNotBeNil)
		So(valid, ShouldBeFalse)

		user, err := Auth("key", "baz")
		So(err, ShouldBeNil)
		So(user.Role, ShouldEqual, testUser.Role)
		So(user.OrgId, ShouldEqual, testUser.OrgId)
		So(user.OrgName, ShouldEqual, testUser.OrgName)
		So(user.OrgSlug, ShouldEqual, testUser.OrgSlug)
		So(user.IsAdmin, ShouldEqual, testUser.IsAdmin)
		So(user.key, ShouldEqual, testUser.key)

		// make sure cache is now updated.
		cuser, valid = cache.Get("baz")
		So(cuser, ShouldNotBeNil)
		So(valid, ShouldBeTrue)

		mockTransport.Reset()
	})
	Convey("When authenticating using expired cache and no g.net response", t, func() {
		cache.Set("baz", &testUser, 0)
		mockTransport.RegisterResponder("POST", "https://grafana.com/api/api-keys/check", func(req *http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("failed")
		})

		// make sure cached item is expired.
		cuser, valid := cache.Get("baz")
		So(cuser, ShouldNotBeNil)
		So(valid, ShouldBeFalse)

		user, err := Auth("key", "baz")
		So(err, ShouldBeNil)
		So(user.Role, ShouldEqual, testUser.Role)
		So(user.OrgId, ShouldEqual, testUser.OrgId)
		So(user.OrgName, ShouldEqual, testUser.OrgName)
		So(user.OrgSlug, ShouldEqual, testUser.OrgSlug)
		So(user.IsAdmin, ShouldEqual, testUser.IsAdmin)
		So(user.key, ShouldEqual, testUser.key)

		// make sure cache is now updated.
		cuser, valid = cache.Get("baz")
		So(cuser, ShouldNotBeNil)
		So(valid, ShouldBeTrue)

		mockTransport.Reset()
	})

}

func TestCheckInstance(t *testing.T) {
	mockTransport := httpmock.NewMockTransport()
	client.Transport = mockTransport

	testUser := SignedInUser{
		Id:        3,
		OrgName:   "awoods Test",
		OrgSlug:   "awoodsTest",
		OrgId:     2,
		Name:      "testKey",
		Role:      ROLE_EDITOR,
		CreatedAt: time.Now(),
		key:       "foo",
	}

	testInstance := Instance{
		ID:    10,
		OrgID: 3,
	}

	Convey("when checking valid instanceID", t, func() {
		responder, err := httpmock.NewJsonResponder(200, &testInstance)
		So(err, ShouldBeNil)
		mockTransport.RegisterResponder("GET", "https://grafana.com/api/hosted-metrics/10", responder)

		err = testUser.CheckInstance("10")
		So(err, ShouldBeNil)
		mockTransport.Reset()
	})

	Convey("when checking cached valid instanceID", t, func() {
		responder, err := httpmock.NewJsonResponder(404, "not found")
		So(err, ShouldBeNil)
		mockTransport.RegisterResponder("GET", "https://grafana.com/api/hosted-metrics/10", responder)

		instanceCache.Set("awoodsTest-10", true, time.Second)
		err = testUser.CheckInstance("10")
		So(err, ShouldEqual, nil)
		mockTransport.Reset()
	})
	Convey("when checking valid instanceID with expired cache and g.com is down", t, func() {
		mockTransport.RegisterResponder("GET", "https://grafana.com/api/hosted-metrics/10", func(req *http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("failed")
		})
		instanceCache.Set("awoodsTest-10", true, 0)
		err := testUser.CheckInstance("10")
		So(err, ShouldEqual, nil)
		mockTransport.Reset()
	})
	Convey("when checking invalid instanceID", t, func() {
		responder, err := httpmock.NewJsonResponder(404, "not found")
		So(err, ShouldBeNil)
		mockTransport.RegisterResponder("GET", "https://grafana.com/api/hosted-metrics/20", responder)

		err = testUser.CheckInstance("20")
		So(err, ShouldEqual, ErrInvalidInstanceID)
		mockTransport.Reset()
	})
}
