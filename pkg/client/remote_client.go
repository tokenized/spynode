package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/tokenized/config"
	"github.com/tokenized/logger"
	"github.com/tokenized/metrics"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/merchant_api"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

var (
	ErrConnectionClosed = errors.New("Connection Closed")

	Endian = binary.LittleEndian

	// RemoteClientVersion is the current version of the communication protocol
	RemoteClientVersion = uint8(0)
)

// RemoteClient is a client for interacting with the spynode service.
type RemoteClient struct {
	config        *Config
	nextMessageID uint64

	// Requests
	requests    []*request
	requestLock sync.Mutex

	accepted, ready bool

	clientID bitcoin.Hash20

	// Session
	hash             bitcoin.Hash32    // for generating session key
	serverSessionKey bitcoin.PublicKey // for this session
	sessionKey       bitcoin.Key
	sessionLock      sync.Mutex

	conn     net.Conn
	connLock sync.Mutex

	handlerChannel     chan *Message
	handlerChannelLock sync.Mutex

	handlers    []Handler
	handlerLock sync.Mutex

	closed bool
	lock   sync.Mutex
}

type request struct {
	typ    uint64
	hash   bitcoin.Hash32
	height int

	id       uint64
	response chan *Message
}

// NewRemoteClient creates a remote client.
// Note: If the connection type is not "full" then it will auto-connect when a function is called to
// communicate with the spynode service. Make sure `Close` is called before application end so that
// the connection can be closed and the listen thread completed.
func NewRemoteClient(config *Config) (*RemoteClient, error) {
	publicKey := config.ClientKey.PublicKey()
	clientID, err := bitcoin.NewHash20(bitcoin.Hash160(publicKey.Bytes()))
	if err != nil {
		return nil, errors.Wrap(err, "client_id")
	}

	result := &RemoteClient{
		config:        config,
		nextMessageID: 1,
		clientID:      *clientID,
	}

	return result, nil
}

// SetupRetry sets the maximum connection retry attempts and delay before failing.
// This can also be set from the config.
func (c *RemoteClient) SetupRetry(max int, delay time.Duration) {
	c.config.MaxRetries = max
	c.config.RetryDelay = config.NewDuration(delay)
}

func (c *RemoteClient) RegisterHandler(h Handler) {
	c.handlerLock.Lock()
	c.handlers = append(c.handlers, h)
	c.handlerLock.Unlock()
}

func (c *RemoteClient) IsAccepted(ctx context.Context) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.accepted
}

// SubscribePushDatas subscribes to transactions containing the specified push datas.
func (c *RemoteClient) SubscribePushDatas(ctx context.Context, pushDatas [][]byte) error {
	m := &SubscribePushData{
		PushDatas: pushDatas,
	}

	logger.Info(ctx, "Sending subscribe push data message")
	return c.sendMessage(ctx, &Message{Payload: m})
}

// UnsubscribePushDatas unsubscribes to transactions containing the specified push datas.
func (c *RemoteClient) UnsubscribePushDatas(ctx context.Context, pushDatas [][]byte) error {
	m := &UnsubscribePushData{
		PushDatas: pushDatas,
	}

	logger.Info(ctx, "Sending unsubscribe push data message")
	return c.sendMessage(ctx, &Message{Payload: m})
}

// SubscribeTx subscribes to information for a specific transaction. Indexes are the indexes of the
// outputs that need to be monitored for spending.
func (c *RemoteClient) SubscribeTx(ctx context.Context, txid bitcoin.Hash32,
	indexes []uint32) error {

	m := &SubscribeTx{
		TxID:    txid,
		Indexes: indexes,
	}

	logger.Info(ctx, "Sending subscribe tx message")
	return c.sendMessage(ctx, &Message{Payload: m})
}

// UnsubscribeTx unsubscribes to information for a specific transaction.
func (c *RemoteClient) UnsubscribeTx(ctx context.Context, txid bitcoin.Hash32,
	indexes []uint32) error {

	m := &UnsubscribeTx{
		TxID:    txid,
		Indexes: indexes,
	}

	logger.Info(ctx, "Sending unsubscribe tx message")
	return c.sendMessage(ctx, &Message{Payload: m})
}

func (c *RemoteClient) SubscribeOutputs(ctx context.Context, outputs []*wire.OutPoint) error {
	m := &SubscribeOutputs{
		Outputs: outputs,
	}

	logger.Info(ctx, "Sending subscribe outputs message")
	return c.sendMessage(ctx, &Message{Payload: m})
}

func (c *RemoteClient) UnsubscribeOutputs(ctx context.Context, outputs []*wire.OutPoint) error {
	m := &UnsubscribeOutputs{
		Outputs: outputs,
	}

	logger.Info(ctx, "Sending unsubscribe outputs message")
	return c.sendMessage(ctx, &Message{Payload: m})
}

// SubscribeHeaders subscribes to information on new block headers.
func (c *RemoteClient) SubscribeHeaders(ctx context.Context) error {
	m := &SubscribeHeaders{}

	logger.Info(ctx, "Sending subscribe headers message")
	return c.sendMessage(ctx, &Message{Payload: m})
}

// UnsubscribeHeaders unsubscribes to information on new block headers.
func (c *RemoteClient) UnsubscribeHeaders(ctx context.Context) error {
	m := &UnsubscribeHeaders{}

	logger.Info(ctx, "Sending unsubscribe headers message")
	return c.sendMessage(ctx, &Message{Payload: m})
}

// SubscribeContracts subscribes to information on contracts.
func (c *RemoteClient) SubscribeContracts(ctx context.Context) error {
	m := &SubscribeContracts{}

	logger.Info(ctx, "Sending subscribe contracts message")
	return c.sendMessage(ctx, &Message{Payload: m})
}

// UnsubscribeContracts unsubscribes to information on contracts.
func (c *RemoteClient) UnsubscribeContracts(ctx context.Context) error {
	m := &UnsubscribeContracts{}

	logger.Info(ctx, "Sending unsubscribe contracts message")
	return c.sendMessage(ctx, &Message{Payload: m})
}

// Ready tells the spynode the client is ready to start receiving updates. Call this after
// connecting and subscribing to all relevant push data.
func (c *RemoteClient) Ready(ctx context.Context, nextMessageID uint64) error {
	if nextMessageID == 0 {
		nextMessageID = 1 // first message id is 1
	}

	m := &Ready{
		NextMessageID: nextMessageID,
	}

	c.lock.Lock()
	c.nextMessageID = nextMessageID
	c.ready = true
	c.lock.Unlock()

	logger.Info(ctx, "Sending ready message (next message %d)", nextMessageID)
	if err := c.sendMessage(ctx, &Message{Payload: m}); err != nil {
		return err
	}
	return nil
}

func (c *RemoteClient) NextMessageID() uint64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.nextMessageID
}

func (c *RemoteClient) SendTx(ctx context.Context, tx *wire.MsgTx) error {
	return c.SendTxAndMarkOutputs(ctx, tx, nil)
}

// SendTxAndMarkOutputs sends a tx message to the bitcoin network. It is synchronous meaning it
// will wait for a response before returning.
func (c *RemoteClient) SendTxAndMarkOutputs(ctx context.Context, tx *wire.MsgTx,
	indexes []uint32) error {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.SendTxAndMarkOutputs")

	// Create request
	txid := *tx.TxHash()
	requestID := rand.Uint64()
	responseChannel := make(chan *Message, 1) // use buffer of 1 to prevent lock on write
	request := &request{
		typ:      MessageTypeSendTx,
		hash:     txid,
		id:       requestID,
		response: responseChannel,
	}

	// Add to requests so when the response is seen it can be matched up.
	c.requestLock.Lock()
	c.requests = append(c.requests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Stringer("send_txid", txid),
	}, "Sending send tx request")
	m := &SendTx{
		Tx:      tx,
		Indexes: indexes,
	}
	if err := c.sendMessage(ctx, &Message{Payload: m}); err != nil {
		return err
	}

	// Wait for response
	select {
	case response := <-responseChannel:
		switch msg := response.Payload.(type) {
		case *Reject:
			rejectErr := NewRejectError(msg.Code, msg.Message)
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Stringer("send_txid", txid),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received reject for send tx request : %s", rejectErr)
			return rejectErr

		case *Accept:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Stringer("send_txid", txid),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received accept for send tx request")
			return nil

		default:
			return fmt.Errorf("Unknown response : %d", response.Payload.Type())
		}

	case <-time.After(c.config.RequestTimeout.Duration):
		// Remove from requests
		c.requestLock.Lock()
		for i, r := range c.requests {
			if r == request {
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				break
			}
		}
		c.requestLock.Unlock()

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("request_id", requestID),
			logger.Stringer("send_txid", txid),
			logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		}, "Timed out waiting for send tx request")
		return ErrTimeout
	}
}

func (c *RemoteClient) PostMerkleProofs(ctx context.Context,
	merkleProofs []*merkle_proof.MerkleProof) error {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.PostMerkleProofs")

	txids := make([]fmt.Stringer, len(merkleProofs))
	blockHashes := make([]fmt.Stringer, len(merkleProofs))
	for i, merkleProof := range merkleProofs {
		txids[i] = merkleProof.GetTxID()
		blockHashes[i] = merkleProof.GetBlockHash()
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringers("txids", txids),
		logger.Stringers("block_hashes", blockHashes),
	}, "Posting merkle proofs")

	m := &PostMerkleProofs{MerkleProofs: merkleProofs}
	if err := c.sendMessage(ctx, &Message{Payload: m}); err != nil {
		return err
	}

	return nil
}

// GetTx requests a tx from the bitcoin network. It is synchronous meaning it will wait for a
// response before returning.
func (c *RemoteClient) GetTx(ctx context.Context, txid bitcoin.Hash32) (*wire.MsgTx, error) {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.GetTx")

	// Create request
	requestID := rand.Uint64()
	responseChannel := make(chan *Message, 1) // use buffer of 1 to prevent lock on write
	request := &request{
		typ:      MessageTypeGetTx,
		hash:     txid,
		id:       requestID,
		response: responseChannel,
	}

	// Add to requests so when the response is seen it can be matched up.
	c.requestLock.Lock()
	c.requests = append(c.requests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Stringer("get_txid", txid),
	}, "Sending get tx request")
	m := &GetTx{TxID: txid}
	if err := c.sendMessage(ctx, &Message{Payload: m}); err != nil {
		return nil, err
	}

	// Wait for response
	select {
	case response := <-responseChannel:
		switch msg := response.Payload.(type) {
		case *Reject:
			rejectErr := NewRejectError(msg.Code, msg.Message)
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Stringer("get_txid", txid),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received reject for get tx request : %s", rejectErr)
			return nil, rejectErr

		case *BaseTx:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Stringer("get_txid", txid),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received base tx for get tx request")
			return msg.Tx, nil

		default:
			return nil, fmt.Errorf("Unknown response : %d", response.Payload.Type())
		}

	case <-time.After(c.config.RequestTimeout.Duration):
		// Remove from requests
		c.requestLock.Lock()
		for i, r := range c.requests {
			if r == request {
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				break
			}
		}
		c.requestLock.Unlock()

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("request_id", requestID),
			logger.Stringer("get_txid", txid),
			logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		}, "Timed out waiting for get tx request")
		return nil, ErrTimeout
	}
}

func (c *RemoteClient) GetOutputs(ctx context.Context,
	outpoints []wire.OutPoint) ([]bitcoin.UTXO, error) {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.GetOutputs")

	outputs := make([]*wire.TxOut, len(outpoints))
	for i, outpoint := range outpoints {
		if outputs[i] != nil {
			continue // already fetched this output
		}

		tx, err := c.GetTx(ctx, outpoint.Hash)
		if err != nil {
			return nil, errors.Wrap(err, "get tx")
		}

		if int(outpoint.Index) >= len(tx.TxOut) {
			return nil, errors.Wrap(err, "invalid index")
		}
		outputs[i] = tx.TxOut[outpoint.Index]

		// Check if other outpoints have the same txid.
		for j := range outpoints[i+1:] {
			if outpoints[j].Hash.Equal(&outpoint.Hash) {
				if int(outpoint.Index) >= len(tx.TxOut) {
					return nil, errors.Wrap(err, "invalid index")
				}
				outputs[j] = tx.TxOut[outpoint.Index]
			}
		}
	}

	result := make([]bitcoin.UTXO, len(outputs))
	for i, output := range outputs {
		result[i] = bitcoin.UTXO{
			Hash:          outpoints[i].Hash,
			Index:         outpoints[i].Index,
			Value:         output.Value,
			LockingScript: output.LockingScript,
		}
	}

	return result, nil
}

// GetHeaders requests a header from the bitcoin network. It is synchronous meaning it will wait for
// a response before returning.
func (c *RemoteClient) GetHeaders(ctx context.Context, height, count int) (*Headers, error) {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.GetHeaders")

	// Create request
	requestID := rand.Uint64()
	responseChannel := make(chan *Message, 1) // use buffer of 1 to prevent lock on write
	request := &request{
		typ:      MessageTypeGetHeaders,
		height:   height,
		id:       requestID,
		response: responseChannel,
	}

	// Add to requests so when the response is seen it can be matched up.
	c.requestLock.Lock()
	c.requests = append(c.requests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Int("request_height", height),
		logger.Int("max_count", count),
	}, "Sending get headers message")
	m := &GetHeaders{
		RequestHeight: int32(height),
		MaxCount:      uint32(count),
	}
	if err := c.sendMessage(ctx, &Message{Payload: m}); err != nil {
		return nil, err
	}

	// Wait for response
	select {
	case response := <-responseChannel:
		switch msg := response.Payload.(type) {
		case *Reject:
			rejectErr := NewRejectError(msg.Code, msg.Message)
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Int("request_height", height),
				logger.Int("max_count", count),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received reject for get headers request : %s", rejectErr)
			return nil, rejectErr

		case *Headers:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Uint32("height", msg.StartHeight),
				logger.Int("count", len(msg.Headers)),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received headers for get headers request")

			return msg, nil

		default:
			return nil, fmt.Errorf("Unknown response : %d", response.Payload.Type())
		}

	case <-time.After(c.config.RequestTimeout.Duration):
		// Remove from requests
		c.requestLock.Lock()
		for i, r := range c.requests {
			if r == request {
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				break
			}
		}
		c.requestLock.Unlock()

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("request_id", requestID),
			logger.Int("request_height", height),
			logger.Int("max_count", count),
			logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		}, "Timed out waiting for get headers request")
		return nil, ErrTimeout
	}
}

func (c *RemoteClient) BlockHash(ctx context.Context, height int) (*bitcoin.Hash32, error) {
	headers, err := c.GetHeaders(ctx, height, 1)
	if err != nil {
		return nil, errors.Wrap(err, "get headers")
	}

	if len(headers.Headers) == 0 {
		return nil, errors.New("No headers returned")
	}

	return headers.Headers[0].BlockHash(), nil
}

func (c *RemoteClient) GetHeader(ctx context.Context, blockHash bitcoin.Hash32) (*Header, error) {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.GetHeader")

	// Create request
	requestID := rand.Uint64()
	responseChannel := make(chan *Message, 1) // use buffer of 1 to prevent lock on write
	request := &request{
		typ:      MessageTypeGetHeader,
		hash:     blockHash,
		id:       requestID,
		response: responseChannel,
	}

	// Add to requests so when the response is seen it can be matched up.
	c.requestLock.Lock()
	c.requests = append(c.requests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Stringer("block_hash", blockHash),
	}, "Sending get header message")
	m := &GetHeader{
		BlockHash: blockHash,
	}
	if err := c.sendMessage(ctx, &Message{Payload: m}); err != nil {
		return nil, err
	}

	// Wait for response
	select {
	case response := <-responseChannel:
		switch msg := response.Payload.(type) {
		case *Reject:
			rejectErr := NewRejectError(msg.Code, msg.Message)
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Stringer("block_hash", blockHash),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received reject for get header request : %s", rejectErr)
			return nil, rejectErr

		case *Header:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Stringer("block_hash", msg.Header.BlockHash()),
				logger.Uint32("block_height", msg.BlockHeight),
				logger.Bool("is_most_pow", msg.IsMostPOW),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received header for get header request")
			return msg, nil

		default:
			return nil, fmt.Errorf("Unknown response : %d", response.Payload.Type())
		}

	case <-time.After(c.config.RequestTimeout.Duration):
		// Remove from requests
		c.requestLock.Lock()
		for i, r := range c.requests {
			if r == request {
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				break
			}
		}
		c.requestLock.Unlock()

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("request_id", requestID),
			logger.Stringer("block_hash", blockHash),
			logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		}, "Timed out waiting for get header request")
		return nil, ErrTimeout
	}
}

func (c *RemoteClient) VerifyMerkleProof(ctx context.Context,
	proof *merkle_proof.MerkleProof) (int, bool, error) {

	var blockHash bitcoin.Hash32
	if proof.BlockHeader != nil {
		blockHash = *proof.BlockHeader.BlockHash()
	} else if proof.BlockHash != nil {
		blockHash = *proof.BlockHash
	} else {
		return -1, false, merkle_proof.ErrNotVerifiable
	}

	header, err := c.GetHeader(ctx, blockHash)
	if err != nil {
		return -1, false, errors.Wrap(err, "get header")
	}

	proof.BlockHeader = &header.Header

	if err := proof.Verify(); err != nil {
		return -1, false, errors.Wrap(err, "merkle proof")
	}

	return int(header.BlockHeight), header.IsMostPOW, nil
}

func (c *RemoteClient) GetFeeQuotes(ctx context.Context) (merchant_api.FeeQuotes, error) {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.GetFeeQuotes")

	// Create request
	requestID := rand.Uint64()
	responseChannel := make(chan *Message, 1) // use buffer of 1 to prevent lock on write
	request := &request{
		typ:      MessageTypeGetFeeQuotes,
		id:       requestID,
		response: responseChannel,
	}

	// Add to requests so when the response is seen it can be matched up.
	c.requestLock.Lock()
	c.requests = append(c.requests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
	}, "Sending get fee quotes message")
	m := &GetFeeQuotes{}
	if err := c.sendMessage(ctx, &Message{Payload: m}); err != nil {
		return nil, err
	}

	// Wait for response
	select {
	case response := <-responseChannel:
		switch msg := response.Payload.(type) {
		case *Reject:
			rejectErr := NewRejectError(msg.Code, msg.Message)
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received reject for get fee quotes request : %s", rejectErr)
			return nil, rejectErr

		case *FeeQuotes:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received fee quotes for get fee quotes request")
			return msg.FeeQuotes, nil

		default:
			return nil, fmt.Errorf("Unknown response : %d", response.Payload.Type())
		}

	case <-time.After(c.config.RequestTimeout.Duration):
		// Remove from requests
		c.requestLock.Lock()
		for i, r := range c.requests {
			if r == request {
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				break
			}
		}
		c.requestLock.Unlock()

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("request_id", requestID),
			logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		}, "Timed out waiting for get fee quotes request")
		return nil, ErrTimeout
	}
}

// ReprocessTx requests that a tx be reprocessed.
func (c *RemoteClient) ReprocessTx(ctx context.Context, txid bitcoin.Hash32,
	clientIDs []bitcoin.Hash20) error {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.ReprocessTx")

	// Create request
	requestID := rand.Uint64()
	responseChannel := make(chan *Message, 1) // use buffer of 1 to prevent lock on write
	request := &request{
		typ:      MessageTypeReprocessTx,
		hash:     txid,
		id:       requestID,
		response: responseChannel,
	}

	// Add to requests so when the response is seen it can be matched up.
	c.requestLock.Lock()
	c.requests = append(c.requests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Stringer("reprocess_txid", txid),
	}, "Sending reprocess tx request")
	m := &ReprocessTx{
		TxID:      txid,
		ClientIDs: clientIDs,
	}
	if err := c.sendMessage(ctx, &Message{Payload: m}); err != nil {
		return err
	}

	// Wait for response
	select {
	case response := <-responseChannel:
		switch msg := response.Payload.(type) {
		case *Reject:
			rejectErr := NewRejectError(msg.Code, msg.Message)
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Stringer("reprocess_txid", txid),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received reject for reprocess tx request : %s", rejectErr)
			return rejectErr

		case *Accept:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Stringer("reprocess_txid", txid),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received accept for reprocess tx request")
			return nil

		default:
			return fmt.Errorf("Unknown response : %d", response.Payload.Type())
		}

	case <-time.After(c.config.RequestTimeout.Duration):
		// Remove from requests
		c.requestLock.Lock()
		for i, r := range c.requests {
			if r == request {
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				break
			}
		}
		c.requestLock.Unlock()

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("request_id", requestID),
			logger.Stringer("reprocess_txid", txid),
			logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		}, "Timed out waiting for reprocess tx request")
		return ErrTimeout
	}
}

// MarkHeaderInvalid request that a block hash is marked as invalid.
func (c *RemoteClient) MarkHeaderInvalid(ctx context.Context, blockHash bitcoin.Hash32) error {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.MarkHeaderInvalid")

	// Create request
	requestID := rand.Uint64()
	responseChannel := make(chan *Message, 1) // use buffer of 1 to prevent lock on write
	request := &request{
		typ:      MessageTypeMarkHeaderInvalid,
		hash:     blockHash,
		id:       requestID,
		response: responseChannel,
	}

	// Add to requests so when the response is seen it can be matched up.
	c.requestLock.Lock()
	c.requests = append(c.requests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Stringer("block_hash", blockHash),
	}, "Sending mark header invalid request")
	m := &MarkHeaderInvalid{
		BlockHash: blockHash,
	}
	if err := c.sendMessage(ctx, &Message{Payload: m}); err != nil {
		return err
	}

	// Wait for response
	select {
	case response := <-responseChannel:
		switch msg := response.Payload.(type) {
		case *Reject:
			rejectErr := NewRejectError(msg.Code, msg.Message)
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Stringer("block_hash", blockHash),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received reject for mark header invalid request : %s", rejectErr)
			return rejectErr

		case *Accept:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Stringer("block_hash", blockHash),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received accept for mark header invalid request")
			return nil

		default:
			return fmt.Errorf("Unknown response : %d", response.Payload.Type())
		}

	case <-time.After(c.config.RequestTimeout.Duration):
		// Remove from requests
		c.requestLock.Lock()
		for i, r := range c.requests {
			if r == request {
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				break
			}
		}
		c.requestLock.Unlock()

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("request_id", requestID),
			logger.Stringer("block_hash", blockHash),
			logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		}, "Timed out waiting for mark header invalid request")
		return ErrTimeout
	}
}

// MarkHeaderNotInvalid request that a block hash is marked as not invalid.
func (c *RemoteClient) MarkHeaderNotInvalid(ctx context.Context, blockHash bitcoin.Hash32) error {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.MarkHeaderNotInvalid")

	// Create request
	requestID := rand.Uint64()
	responseChannel := make(chan *Message, 1) // use buffer of 1 to prevent lock on write
	request := &request{
		typ:      MessageTypeMarkHeaderNotInvalid,
		hash:     blockHash,
		id:       requestID,
		response: responseChannel,
	}

	// Add to requests so when the response is seen it can be matched up.
	c.requestLock.Lock()
	c.requests = append(c.requests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Stringer("block_hash", blockHash),
	}, "Sending mark header not invalid request")
	m := &MarkHeaderNotInvalid{
		BlockHash: blockHash,
	}
	if err := c.sendMessage(ctx, &Message{Payload: m}); err != nil {
		return err
	}

	// Wait for response
	select {
	case response := <-responseChannel:
		switch msg := response.Payload.(type) {
		case *Reject:
			rejectErr := NewRejectError(msg.Code, msg.Message)
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Stringer("block_hash", blockHash),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received reject for mark header not invalid request : %s", rejectErr)
			return rejectErr

		case *Accept:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Stringer("block_hash", blockHash),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received accept for mark header not invalid request")
			return nil

		default:
			return fmt.Errorf("Unknown response : %d", response.Payload.Type())
		}

	case <-time.After(c.config.RequestTimeout.Duration):
		// Remove from requests
		c.requestLock.Lock()
		for i, r := range c.requests {
			if r == request {
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				break
			}
		}
		c.requestLock.Unlock()

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("request_id", requestID),
			logger.Stringer("block_hash", blockHash),
			logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		}, "Timed out waiting for mark header not invalid request")
		return ErrTimeout
	}
}

// ping sends pings to keep the connection alive.
func (c *RemoteClient) ping(ctx context.Context, interrupt <-chan interface{}) error {
	for {
		select {
		case <-interrupt:
			return nil

		case <-time.After(2 * time.Minute):
			timeStamp := uint64(time.Now().UnixNano())
			m := &Ping{
				TimeStamp: timeStamp,
			}
			if err := c.sendMessage(ctx, &Message{Payload: m}); err != nil {
				if errors.Cause(err) == ErrConnectionClosed {
					continue
				}
				return errors.Wrap(err, "send")
			}
			logger.VerboseWithFields(ctx, []logger.Field{
				logger.Float64("timestamp", float64(timeStamp)/1000000000.0),
			}, "Sent ping")
		}
	}
}

func (c *RemoteClient) connect(ctx context.Context) (net.Conn, error) {
	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("client_id", c.clientID),
	}, "Connecting to spynode service")

	sessionHash, err := c.generateSession()
	if err != nil {
		return nil, errors.Wrap(err, "session")
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", c.config.ServerAddress)
	if err != nil {
		return nil, errors.Wrap(err, "dial")
	}

	// Create and sign register message
	register := &Register{
		Version:          RemoteClientVersion,
		Key:              c.config.ClientKey.PublicKey(),
		Hash:             *sessionHash,
		StartBlockHeight: c.config.StartBlockHeight,
		ConnectionType:   c.config.ConnectionType,
	}

	sigHash, err := register.SigHash()
	if err != nil {
		conn.Close()
		return nil, errors.Wrap(err, "sig hash")
	}

	register.Signature, err = c.config.ClientKey.Sign(*sigHash)
	if err != nil {
		conn.Close()
		return nil, errors.Wrap(err, "sign")
	}

	message := Message{Payload: register}
	if err := message.Serialize(conn); err != nil {
		conn.Close()
		return nil, errors.Wrap(err, "send register")
	}

	return conn, nil
}

// generateSession generates session keys from root keys.
func (c *RemoteClient) generateSession() (*bitcoin.Hash32, error) {
	c.sessionLock.Lock()
	defer c.sessionLock.Unlock()

	for { // loop through any out of range keys
		var err error

		// Generate random hash
		c.hash, err = bitcoin.GenerateSeedValue()
		if err != nil {
			return nil, errors.Wrap(err, "generate hash")
		}

		// Derive session keys
		c.serverSessionKey, err = bitcoin.NextPublicKey(c.config.ServerKey, c.hash)
		if err != nil {
			if errors.Cause(err) == bitcoin.ErrOutOfRangeKey {
				continue // try with a new hash
			}
			return nil, errors.Wrap(err, "next public key")
		}

		c.sessionKey, err = bitcoin.NextKey(c.config.ClientKey, c.hash)
		if err != nil {
			if errors.Cause(err) == bitcoin.ErrOutOfRangeKey {
				continue // try with a new hash
			}
			return nil, errors.Wrap(err, "next key")
		}

		return &c.hash, nil
	}
}

func (c *RemoteClient) maintainConnection(ctx context.Context, receiveMessagesChannel chan *Message,
	interrupt <-chan interface{}) error {

	first := true
	lastConnection := time.Now()
	for {
		if first {
			first = false
		} else {
			since := time.Since(lastConnection)
			if since > c.config.RetryError.Duration {
				logger.ErrorWithFields(ctx, []logger.Field{
					logger.MillisecondsFromNano("delay", since.Nanoseconds()),
				}, "Failing to connect to spynode service")
			}

			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("delay", c.config.RetryDelay),
			}, "Delaying before retrying connection")

			select {
			case <-interrupt:
				return threads.Interrupted
			case <-time.After(c.config.RetryDelay.Duration):
			}
		}

		conn, err := c.connect(ctx)
		if err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("client_id", c.clientID),
			}, "Failed to connect to spynode service : %s", err)
			continue
		}

		lastConnection = time.Now()

		var wait sync.WaitGroup

		c.connLock.Lock()
		c.conn = conn
		c.connLock.Unlock()

		receiveThread, receiveComplete := threads.NewUninterruptableThreadComplete("SpyNode Receive",
			func(ctx context.Context) error {
				return receiveMessages(ctx, conn, receiveMessagesChannel)
			}, &wait)

		receiveThread.Start(ctx)

		stop := false
		select {
		case receiveErr := <-receiveComplete:
			if receiveErr != nil {
				if errors.Cause(receiveErr) == io.EOF ||
					errors.Cause(receiveErr) == io.ErrUnexpectedEOF ||
					strings.Contains(receiveErr.Error(), "Closed") ||
					strings.Contains(receiveErr.Error(), "use of closed network connection") {
					logger.Info(ctx, "Disconnected")
				} else {
					if _, ok := errors.Cause(receiveErr).(RejectError); ok {
						logger.Error(ctx, "Connection rejected : %s", receiveErr)
						return receiveErr
					} else {
						logger.Warn(ctx, "Receive messages failed : %s", receiveErr)
					}
				}
			} else {
				logger.Warn(ctx, "Receive messages completed")
			}

		case <-interrupt:
			stop = true
		}

		c.connLock.Lock()
		c.conn = nil
		c.connLock.Unlock()

		conn.Close()

		wait.Wait()
		if stop {
			return threads.Interrupted
		}
	}
}

func (c *RemoteClient) Run(ctx context.Context, interrupt <-chan interface{}) error {
	c.lock.Lock()
	clientID := c.clientID
	c.lock.Unlock()

	defer func() {
		c.lock.Lock()
		c.closed = true
		c.lock.Unlock()
	}()

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("client_id", clientID))
	defer logger.Info(ctx, "Client connection completed")

	receiveMessagesChannel := make(chan *Message, 100)

	c.handlerLock.Lock()
	c.handlerChannel = make(chan *Message, 100)
	c.handlerLock.Unlock()

	var wait, connectionWait sync.WaitGroup
	var stopper threads.StopCombiner

	handleMessagesThread, handleMessagesComplete := threads.NewUninterruptableThreadComplete("SpyNode Handle Messages",
		func(ctx context.Context) error {
			return c.handleMessages(ctx, receiveMessagesChannel)
		}, &wait)

	handlerThread, handlerComplete := threads.NewUninterruptableThreadComplete("SpyNode Handler",
		func(ctx context.Context) error {
			return c.runHandler(ctx, c.handlerChannel)
		}, &wait)

	pingThread, pingComplete := threads.NewInterruptableThreadComplete("SpyNode Ping", c.ping,
		&wait)
	stopper.Add(pingThread)

	connectionThread, connectionComplete := threads.NewInterruptableThreadComplete("SpyNode Connection",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return c.maintainConnection(ctx, receiveMessagesChannel, interrupt)
		}, &connectionWait)
	stopper.Add(connectionThread)

	handleMessagesThread.Start(ctx)
	handlerThread.Start(ctx)
	pingThread.Start(ctx)
	connectionThread.Start(ctx)

	select {
	case handleMessagesErr := <-handleMessagesComplete:
		if handleMessagesErr != nil {
			logger.Warn(ctx, "Handle messages failed : %s", handleMessagesErr)
		} else {
			logger.Warn(ctx, "Client handle messages completed")
		}

	case handlerErr := <-handlerComplete:
		if handlerErr != nil {
			logger.Warn(ctx, "Handler failed : %s", handlerErr)
		} else {
			logger.Warn(ctx, "Client handler completed")
		}

	case pingErr := <-pingComplete:
		if pingErr != nil {
			logger.Warn(ctx, "Ping failed : %s", pingErr)
		} else {
			logger.Warn(ctx, "Ping completed")
		}

	case connectionErr := <-connectionComplete:
		if connectionErr != nil {
			logger.Warn(ctx, "Connection failed : %s", connectionErr)
		} else {
			logger.Warn(ctx, "Connection completed")
		}

	case <-interrupt:
	}

	stopper.Stop(ctx)

	c.handlerLock.Lock()
	close(c.handlerChannel)
	c.handlerChannel = nil
	c.handlerLock.Unlock()

	connectionWait.Wait()
	close(receiveMessagesChannel)

	wait.Wait()

	return threads.CombineErrors(
		handleMessagesThread.Error(),
		handlerThread.Error(),
		pingThread.Error(),
		connectionThread.Error(),
	)
}

func (c *RemoteClient) sendMessage(ctx context.Context, msg *Message) error {
	for i := 0; ; i++ {
		err := c.attemptSendMessage(ctx, msg)
		if err == nil {
			return nil
		}

		if i == c.config.MaxRetries {
			return err
		}

		c.lock.Lock()
		closed := c.closed
		c.lock.Unlock()

		if closed {
			return err
		}

		time.Sleep(c.config.RetryDelay.Duration)
	}
}

func (c *RemoteClient) attemptSendMessage(ctx context.Context, msg *Message) error {
	c.connLock.Lock()
	defer c.connLock.Unlock()

	if c.conn == nil {
		return ErrConnectionClosed
	}

	if err := msg.Serialize(c.conn); err != nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.String("message", NameForMessageType(msg.Payload.Type())),
		}, "Failed to send message : %s", err)
		c.conn.Close()
		c.conn = nil
		return err
	}

	return nil
}

func (c *RemoteClient) addHandlerMessage(ctx context.Context, msg *Message) error {
	c.handlerChannelLock.Lock()
	defer c.handlerChannelLock.Unlock()
	if c.handlerChannel == nil {
		return ErrConnectionClosed
	}

	c.handlerChannel <- msg
	return nil
}

func receiveMessages(ctx context.Context, conn net.Conn, channel chan<- *Message) error {
	for {
		message := &Message{}
		if err := message.Deserialize(conn); err != nil {
			return err
		}

		channel <- message
	}
}

func (c *RemoteClient) handleMessages(ctx context.Context, incomingMessages <-chan *Message) error {
	for msg := range incomingMessages {
		if err := c.handleMessage(ctx, msg); err != nil {
			return err
		}
	}

	return nil
}

func (c *RemoteClient) handleMessage(ctx context.Context, m *Message) error {
	messageName, exists := MessageTypeNames[m.Payload.Type()]
	if !exists {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Uint64("remote_message", m.Payload.Type()),
		}, "Name not known for message type")
		ctx = logger.ContextWithLogFields(ctx, logger.Uint64("remote_message", m.Payload.Type()))
	} else {
		ctx = logger.ContextWithLogFields(ctx, logger.String("remote_message", messageName))
	}

	// Handle message
	switch msg := m.Payload.(type) {
	case *AcceptRegister:
		logger.Info(ctx, "Received accept register")
		if !msg.Key.Equal(c.serverSessionKey) {
			logger.Error(ctx, "Wrong server session key returned : got %s, want %s", msg.Key,
				c.serverSessionKey)
			return ErrWrongKey
		}

		sigHash, err := msg.SigHash(c.hash)
		if err != nil {
			return errors.Wrap(err, "accept sig hash")
		}

		if !msg.Signature.Verify(*sigHash, msg.Key) {
			return ErrBadSignature
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.JSON("accept_register", msg),
		}, "Server accepted connection")
		c.lock.Lock()
		c.accepted = true
		c.lock.Unlock()

		c.addHandlerMessage(ctx, m)

	case *Tx:
		txid := *msg.Tx.TxHash()
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txid),
			logger.Uint64("message_id", msg.ID),
		}, "Received tx")

		if c.nextMessageID != msg.ID {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Uint64("expected_message_id", c.nextMessageID),
				logger.Uint64("message_id", msg.ID),
			}, "Wrong message ID in tx message")
		} else {
			c.nextMessageID = msg.ID + 1
			c.addHandlerMessage(ctx, m)
		}

	case *TxUpdate:
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("txid", msg.TxID),
			logger.Uint64("message_id", msg.ID),
		}, "Received tx state")

		if c.nextMessageID != msg.ID {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Uint64("expected_message_id", c.nextMessageID),
				logger.Uint64("message_id", msg.ID),
			}, "Wrong message ID in tx update message")
		} else {
			c.nextMessageID = msg.ID + 1
			c.addHandlerMessage(ctx, m)
		}

	case *Headers:
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Int("header_count", len(msg.Headers)),
			logger.Uint32("start_height", msg.StartHeight),
		}, "Received headers")

		found := false
		c.requestLock.Lock()
		for i, request := range c.requests {
			if request.typ == MessageTypeGetHeaders && request.height == int(msg.RequestHeight) {
				request.response <- m
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				found = true
				break
			}
		}
		c.requestLock.Unlock()

		if !found {
			c.addHandlerMessage(ctx, m)
		}

	case *Header:
		blockHash := *msg.Header.BlockHash()
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("block_hash", blockHash),
		}, "Received header")

		c.requestLock.Lock()
		for i, request := range c.requests {
			if request.typ == MessageTypeGetHeaders && request.hash.Equal(&blockHash) {
				request.response <- m
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				break
			}
		}
		c.requestLock.Unlock()

	case *FeeQuotes:
		logger.Info(ctx, "Received fee quotes")

		c.requestLock.Lock()
		for i, request := range c.requests {
			if request.typ == MessageTypeGetFeeQuotes {
				request.response <- m
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				break
			}
		}
		c.requestLock.Unlock()

		c.addHandlerMessage(ctx, m)

	case *InSync:
		logger.Info(ctx, "Received in sync")

		c.addHandlerMessage(ctx, m)

	case *ChainTip:
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("hash", msg.Hash),
			logger.Uint32("height", msg.Height),
		}, "Received chain tip")

		c.addHandlerMessage(ctx, m)

	case *BaseTx:
		txid := *msg.Tx.TxHash()
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txid),
		}, "Received base tx")

		c.requestLock.Lock()
		found := false
		for i, request := range c.requests {
			if request.typ == MessageTypeGetTx && request.hash.Equal(&txid) {
				request.response <- m
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				break
			}
		}
		c.requestLock.Unlock()

		if !found {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", txid),
			}, "No matching request found for base tx")
		}

	case *Accept:
		logger.Info(ctx, "Received accept")
		if msg.Hash != nil {
			switch msg.MessageType {
			case MessageTypeSendTx:
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Stringer("txid", msg.Hash),
				}, "Received accept for send tx")

				found := false
				c.requestLock.Lock()
				for i, request := range c.requests {
					if request.typ == MessageTypeSendTx && request.hash.Equal(msg.Hash) {
						request.response <- m
						c.requests = append(c.requests[:i], c.requests[i+1:]...)
						found = true
						break
					}
				}
				c.requestLock.Unlock()

				if !found {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Stringer("txid", msg.Hash),
					}, "No matching request found for send tx accept")
				}

			case MessageTypeReprocessTx:
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Stringer("txid", msg.Hash),
				}, "Received accept for reprocess tx")

				found := false
				c.requestLock.Lock()
				for i, request := range c.requests {
					if request.typ == MessageTypeReprocessTx && request.hash.Equal(msg.Hash) {
						request.response <- m
						c.requests = append(c.requests[:i], c.requests[i+1:]...)
						found = true
						break
					}
				}
				c.requestLock.Unlock()

				if !found {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Stringer("txid", msg.Hash),
					}, "No matching request found for mark header invalid accept")
				}

			case MessageTypeMarkHeaderInvalid:
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Stringer("block_hash", msg.Hash),
				}, "Received accept for mark header invalid")

				found := false
				c.requestLock.Lock()
				for i, request := range c.requests {
					if request.typ == MessageTypeMarkHeaderInvalid && request.hash.Equal(msg.Hash) {
						request.response <- m
						c.requests = append(c.requests[:i], c.requests[i+1:]...)
						found = true
						break
					}
				}
				c.requestLock.Unlock()

				if !found {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Stringer("block_hash", msg.Hash),
					}, "No matching request found for mark header invalid accept")
				}

			case MessageTypeMarkHeaderNotInvalid:
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Stringer("block_hash", msg.Hash),
				}, "Received accept for mark header not invalid")

				found := false
				c.requestLock.Lock()
				for i, request := range c.requests {
					if request.typ == MessageTypeMarkHeaderNotInvalid && request.hash.Equal(msg.Hash) {
						request.response <- m
						c.requests = append(c.requests[:i], c.requests[i+1:]...)
						found = true
						break
					}
				}
				c.requestLock.Unlock()

				if !found {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Stringer("block_hash", msg.Hash),
					}, "No matching request found for mark header not invalid accept")
				}

			default:
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Uint64("type", msg.MessageType),
					logger.String("name", NameForMessageType(msg.MessageType)),
					logger.Stringer("hash", msg.Hash),
				}, "Received accept for unsupported message")

			}
		} else {
			logger.Info(ctx, "Received accept with no hash")
		}

	case *Reject:
		c.lock.Lock()
		accepted := c.accepted
		c.lock.Unlock()

		logger.Info(ctx, "Received reject")

		if !accepted {
			// Service rejected registration
			logger.Info(ctx, "Reject registration")
			return NewRejectError(msg.Code, msg.Message)
		}

		if msg.Hash != nil {
			found := false

			switch msg.MessageType {
			case MessageTypeSendTx:
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("txid", msg.Hash),
				}, "Received reject for send tx : %s", msg.Message)

				c.requestLock.Lock()
				for i, request := range c.requests {
					if request.typ == MessageTypeSendTx && request.hash.Equal(msg.Hash) {
						request.response <- m
						c.requests = append(c.requests[:i], c.requests[i+1:]...)
						found = true
						break
					}
				}
				c.requestLock.Unlock()

				if !found {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Stringer("txid", msg.Hash),
					}, "No matching request found for send tx reject")
				}

			case MessageTypeGetTx:
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("txid", msg.Hash),
				}, "Received reject for get tx : %s", msg.Message)

				c.requestLock.Lock()
				for i, request := range c.requests {
					if request.typ == MessageTypeGetTx && request.hash.Equal(msg.Hash) {
						request.response <- m
						c.requests = append(c.requests[:i], c.requests[i+1:]...)
						found = true
						break
					}
				}
				c.requestLock.Unlock()

				if !found {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Stringer("txid", msg.Hash),
					}, "No matching request found for get tx reject")
				}

			case MessageTypeGetHeader:
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("block_hash", msg.Hash),
				}, "Received reject for get header : %s", msg.Message)

				c.requestLock.Lock()
				for i, request := range c.requests {
					if request.typ == MessageTypeGetHeader && request.hash.Equal(msg.Hash) {
						request.response <- m
						c.requests = append(c.requests[:i], c.requests[i+1:]...)
						found = true
						break
					}
				}
				c.requestLock.Unlock()

				if !found {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Stringer("txid", msg.Hash),
					}, "No matching request found for get header reject")
				}

			case MessageTypeGetFeeQuotes:
				logger.Warn(ctx, "Received reject for get fee quotes : %s", msg.Message)

				c.requestLock.Lock()
				for i, request := range c.requests {
					if request.typ == MessageTypeGetFeeQuotes {
						request.response <- m
						c.requests = append(c.requests[:i], c.requests[i+1:]...)
						break
					}
				}
				c.requestLock.Unlock()

			case MessageTypeReprocessTx:
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("txid", msg.Hash),
				}, "Received reject for reprocess tx : %s", msg.Message)

				c.requestLock.Lock()
				for i, request := range c.requests {
					if request.typ == MessageTypeReprocessTx && request.hash.Equal(msg.Hash) {
						request.response <- m
						c.requests = append(c.requests[:i], c.requests[i+1:]...)
						found = true
						break
					}
				}
				c.requestLock.Unlock()

				if !found {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Stringer("txid", msg.Hash),
					}, "No matching request found for reprocess tx reject")
				}

			case MessageTypeMarkHeaderInvalid:
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Stringer("block_hash", msg.Hash),
				}, "Received reject for mark header invalid")

				found := false
				c.requestLock.Lock()
				for i, request := range c.requests {
					if request.typ == MessageTypeMarkHeaderInvalid && request.hash.Equal(msg.Hash) {
						request.response <- m
						c.requests = append(c.requests[:i], c.requests[i+1:]...)
						found = true
						break
					}
				}
				c.requestLock.Unlock()

				if !found {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Stringer("block_hash", msg.Hash),
					}, "No matching request found for mark header invalid reject")
				}

			case MessageTypeMarkHeaderNotInvalid:
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Stringer("block_hash", msg.Hash),
				}, "Received reject for mark header not invalid")

				found := false
				c.requestLock.Lock()
				for i, request := range c.requests {
					if request.typ == MessageTypeMarkHeaderNotInvalid && request.hash.Equal(msg.Hash) {
						request.response <- m
						c.requests = append(c.requests[:i], c.requests[i+1:]...)
						found = true
						break
					}
				}
				c.requestLock.Unlock()

				if !found {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Stringer("block_hash", msg.Hash),
					}, "No matching request found for mark header not invalid reject")
				}

			default:
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Uint64("type", msg.MessageType),
					logger.String("name", NameForMessageType(msg.MessageType)),
					logger.Stringer("hash", msg.Hash),
				}, "Received reject for unsupported message")
			}
		} else {
			logger.Info(ctx, "Received reject with no hash")
		}

	case *Ping:
		logger.VerboseWithFields(ctx, []logger.Field{
			logger.Float64("timestamp", float64(msg.TimeStamp)/1000000000.0),
		}, "Received ping")

	case *Pong:
		logger.VerboseWithFields(ctx, []logger.Field{
			logger.Float64("request_timestamp", float64(msg.RequestTimeStamp)/1000000000.0),
			logger.Float64("timestamp", float64(msg.TimeStamp)/1000000000.0),
			logger.Float64("delta", float64(msg.TimeStamp-msg.RequestTimeStamp)/1000000000.0),
		}, "Received pong")

	default:
		return fmt.Errorf("Unknown message type : %d (%s)", msg.Type(),
			NameForMessageType(msg.Type()))

	}

	return nil
}

func (c *RemoteClient) runHandler(ctx context.Context, channel <-chan *Message) error {
	for msg := range channel {
		switch payload := msg.Payload.(type) {
		case *AcceptRegister, *ChainTip:
			c.handlerLock.Lock()
			for _, handler := range c.handlers {
				handler.HandleMessage(ctx, payload)
			}
			c.handlerLock.Unlock()

		case *InSync:
			c.handlerLock.Lock()
			for _, handler := range c.handlers {
				handler.HandleInSync(ctx)
			}
			c.handlerLock.Unlock()

		case *Headers:
			c.handlerLock.Lock()
			for _, handler := range c.handlers {
				handler.HandleHeaders(ctx, payload)
			}
			c.handlerLock.Unlock()

		case *TxUpdate:
			c.handlerLock.Lock()
			for _, handler := range c.handlers {
				handler.HandleTxUpdate(ctx, payload)
			}
			c.handlerLock.Unlock()

		case *Tx:
			c.handlerLock.Lock()
			for _, handler := range c.handlers {
				handler.HandleTx(ctx, payload)
			}
			c.handlerLock.Unlock()
		}
	}

	return nil
}
