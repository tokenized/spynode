package handlers

import (
	"bytes"
	"context"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
)

// ExtendedHandler exists to handle the extmsg command.
type ExtendedHandler struct {
	blockHandler MessageHandler
	txHandler    MessageHandler
}

func NewExtendedHandler(blockHandler, txHandler MessageHandler) *ExtendedHandler {
	return &ExtendedHandler{
		blockHandler: blockHandler,
		txHandler:    txHandler,
	}
}

// Handle implements the handler interface for transaction handler.
func (handler *ExtendedHandler) Handle(ctx context.Context,
	m wire.Message) ([]wire.Message, error) {

	msg, ok := m.(*wire.MsgExtended)
	if !ok {
		return nil, errors.New("Could not assert as *wire.MsgExtended")
	}

	switch msg.ExtCommand {
	case wire.CmdBlock:
		if handler.blockHandler == nil {
			// No handler for this message type. Ignore it.
			return nil, nil
		}

		blockMsg := &wire.MsgBlock{}
		if err := blockMsg.BtcDecode(bytes.NewReader(msg.Payload), 0); err != nil {
			return nil, errors.Wrap(err, "decode block message")
		}

		return handler.blockHandler.Handle(ctx, blockMsg)

	case wire.CmdTx:
		if handler.txHandler == nil {
			// No handler for this message type. Ignore it.
			return nil, nil
		}

		txMsg := &wire.MsgTx{}
		if err := txMsg.BtcDecode(bytes.NewReader(msg.Payload), 0); err != nil {
			return nil, errors.Wrap(err, "decode tx message")
		}

		return handler.txHandler.Handle(ctx, txMsg)

	default:
		logger.Error(ctx, "Unsupported extended message command : %s", msg.ExtCommand)
	}

	return nil, nil
}
