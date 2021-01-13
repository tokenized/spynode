package client

import (
	"context"

	"github.com/pkg/errors"
	"github.com/tokenized/pkg/wire"
)

var (
	ErrUnknownMessageType = errors.New("Unknown Message Type")
	ErrInvalid            = errors.New("Invalid")
	ErrIncomplete         = errors.New("Incomplete") // merkle proof is incomplete
	ErrWrongHash          = errors.New("Wrong Hash") // Non-matching merkle root hash
	ErrNotConnected       = errors.New("Not Connected")
	ErrWrongKey           = errors.New("Wrong Key") // The wrong key was provided during auth
	ErrBadSignature       = errors.New("Bad Signature")
	ErrTimeout            = errors.New("Timeout")
	ErrReject             = errors.New("Reject")
)

// Handler provides an interface for handling data from the spynode client.
type Handler interface {
	HandleTx(context.Context, *Tx)
	HandleTxUpdate(context.Context, *TxUpdate)
	HandleHeaders(context.Context, *Headers)
	HandleInSync(context.Context)
}

// Client is the interface for interacting with a spynode.
type Client interface {
	RegisterHandler(Handler)

	SubscribePushDatas(context.Context, [][]byte) error
	UnsubscribePushDatas(context.Context, [][]byte) error
	SubscribeContracts(context.Context) error
	UnsubscribeContracts(context.Context) error
	SubscribeHeaders(context.Context) error
	UnsubscribeHeaders(context.Context) error

	SendTx(context.Context, *wire.MsgTx) error

	Ready(context.Context) error

	SetupRetry(max, delay int)
}