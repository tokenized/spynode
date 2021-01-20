package client

import (
	"context"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
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

	// HandleMessage handles all other client data messages
	HandleMessage(context.Context, MessagePayload)
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

	GetTx(context.Context, bitcoin.Hash32) (*wire.MsgTx, error)
	GetOutputs(context.Context, []wire.OutPoint) ([]bitcoin.UTXO, error)

	SendTx(context.Context, *wire.MsgTx) error

	GetHeaders(context.Context, int, int) ([]*wire.BlockHeader, error)
	BlockHash(context.Context, int) (*bitcoin.Hash32, error)

	Ready(context.Context, uint64) error
}

func SubscribeAddresses(ctx context.Context, ras []bitcoin.RawAddress, cl Client) error {
	pds := make([][]byte, 0, len(ras))
	for _, ra := range ras {
		hashes, err := ra.Hashes()
		if err != nil {
			return errors.Wrap(err, "address hashes")
		}

		for _, hash := range hashes {
			pds = append(pds, hash[:])
		}
	}

	return cl.SubscribePushDatas(ctx, pds)
}
