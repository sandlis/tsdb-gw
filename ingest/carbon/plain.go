package carbon

import (
	"net"

	"github.com/graphite-ng/carbon-relay-ng/input"
	log "github.com/sirupsen/logrus"
)

// PlainHandler extends the carbon-relay-ng plain handler in 2 ways:
// 1) it logs connection start and end via Info level. whereas carbon-relay-ng would consider this debug level
//    (which itself is largely a consequence of the crng stats library creating a new connection to itself at every flush)
// 2) it helps us do stats our way, with the metrics we care about and the library we use (crng uses a different library)
type PlainHandler struct {
	input.Plain
}

func (p *PlainHandler) HandleConn(c net.Conn) {
	carbonConnections.Inc()
	log.Infof("plain handler: new tcp connection from %v", c.RemoteAddr())
	err := p.Handle(c)
	carbonConnections.Dec()

	var remoteInfo string

	rAddr := c.RemoteAddr()
	if rAddr != nil {
		remoteInfo = " for " + rAddr.String()
	}
	if err != nil {
		log.Warnf("plain handler%s returned: %s. closing conn", remoteInfo, err)
		return
	}
	log.Infof("plain handler%s returned. closing conn", remoteInfo)
}
