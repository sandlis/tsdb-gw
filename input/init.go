package input

import (
	"net"
)

type Handler interface {
	Handle(net.Conn)
}
