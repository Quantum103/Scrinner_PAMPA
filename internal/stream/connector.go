package stream

import (
	"context"
	"time"

	"github.com/gorilla/websocket"
)

type gorillaConnector struct {
	dialer *websocket.Dialer
}

func newGorillaConnector() connector {
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second
	return &gorillaConnector{dialer: &dialer}
}

func (c *gorillaConnector) Dial(ctx context.Context, url string) (wireConn, error) {
	conn, response, err := c.dialer.DialContext(ctx, url, nil)
	if err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		return nil, err
	}
	return conn, nil
}
