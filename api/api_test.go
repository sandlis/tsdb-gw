package api

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/raintank/tsdb-gw/api/models"
	"github.com/raintank/tsdb-gw/auth"
	"golang.org/x/net/http/httpguts"
)

func upgradeType(h http.Header) string {
	if !httpguts.HeaderValuesContainsToken(h["Connection"], "Upgrade") {
		return ""
	}
	return strings.ToLower(h.Get("Upgrade"))
}

func createWSBackendServer(t *testing.T) *httptest.Server {
	return httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if upgradeType(r.Header) != "websocket" {
			t.Error("unexpected backend request")
			http.Error(w, "unexpected request", 400)
			return
		}

		c, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close()
		io.WriteString(c, "HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: WebSocket\r\n\r\n")
		bs := bufio.NewScanner(c)
		if !bs.Scan() {
			t.Errorf("backend failed to read line from client: %v", bs.Err())
			return
		}
		fmt.Fprintf(c, "backend got %q\n", bs.Text())
	}))

}

func TestReverseProxyWebSocketWithHijacker(t *testing.T) {
	*addr = ":0"

	backendServer := createWSBackendServer(t)
	backendServer.Start()
	defer backendServer.Close()

	backURL, _ := url.Parse(backendServer.URL)
	rproxy := httputil.NewSingleHostReverseProxy(backURL)

	a := New("grafana-instance", "test-ws")
	a.Router.Any("/ws", a.GenerateHandlers("read", false, false, a.PromStats("cortex-read"), func(c *models.Context) {
		c.Req.Request.Header.Set("X-Scope-OrgID", strconv.Itoa(c.User.ID))
		rproxy.ServeHTTP(c.Resp, c.Req.Request)
	})...)

	frontendProxy := httptest.Server{
		Listener: a.l,
		Config:   &http.Server{Handler: a.Router},
	}
	frontendProxy.Start()
	defer frontendProxy.Close()

	req, _ := http.NewRequest("GET", frontendProxy.URL+"/ws", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.SetBasicAuth("1", auth.AdminKey)

	c := frontendProxy.Client()
	res, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != 101 {
		t.Fatalf("status = %v; want 101", res.Status)
	}

	rwc, ok := res.Body.(io.ReadWriteCloser)
	if !ok {
		t.Fatalf("response body is of type %T; does not implement ReadWriteCloser", res.Body)
	}
	defer rwc.Close()

	io.WriteString(rwc, "Hello\n")
	bs := bufio.NewScanner(rwc)
	if !bs.Scan() {
		t.Fatalf("Scan: %v", bs.Err())
	}

	got := bs.Text()
	want := `backend got "Hello"`
	if got != want {
		t.Errorf("got %#q, want %#q", got, want)
	}
}

type ResponseWriterWithoutHijacker struct {
	http.ResponseWriter
}

func (rw *ResponseWriterWithoutHijacker) Write(b []byte) (int, error) {
	return rw.ResponseWriter.Write(b)
}

func TestReverseProxyWebSocketWithoutHijacker(t *testing.T) {
	*addr = ":0"

	backendServer := createWSBackendServer(t)
	backendServer.Start()
	defer backendServer.Close()

	backURL, _ := url.Parse(backendServer.URL)
	rproxy := httputil.NewSingleHostReverseProxy(backURL)

	frontendProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rproxy.ServeHTTP(&ResponseWriterWithoutHijacker{w}, r)
	}))

	defer frontendProxy.Close()

	req, _ := http.NewRequest("GET", frontendProxy.URL+"/ws", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.SetBasicAuth("1", auth.AdminKey)

	c := frontendProxy.Client()
	res, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != 502 {
		t.Fatalf("status = %v; want 502", res.Status)
	}
}
