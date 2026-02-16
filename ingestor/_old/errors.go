package ingestor

import "errors"

// Error taxonomy. These sentinels enable consistent metrics, logs, and retry rules.
var (
	ErrTransform = errors.New("transform error")
	ErrEncode    = errors.New("encode error")
	ErrSinkWrite = errors.New("sink write error")
	ErrAck       = errors.New("ack error")
)
