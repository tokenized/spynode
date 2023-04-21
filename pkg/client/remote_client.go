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
	"sync/atomic"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/metrics"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/merchant_api"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

var (
	ErrConnectionClosed = errors.New("Connection Closed")
	ErrRequestNotFound  = errors.New("Request Not Found")

	Endian = binary.LittleEndian

	// RemoteClientVersion is the current version of the communication protocol
	RemoteClientVersion = uint8(0)
)

// RemoteClient is a client for interacting with the spynode service.
type RemoteClient struct {
	config           atomic.Value
	requestTimeout   atomic.Value
	messageTimeout   atomic.Value
	handshakeTimeout atomic.Value
	retryConfig      atomic.Value

	nextMessageID atomic.Value

	// Requests
	addRequestsChannel     chan *request
	removeRequestsChannel  chan *request
	requestResponseChannel chan *requestResponse
	requests               []*request

	sendChannel              chan *sendMessageRequest
	handshakeCompleteChannel atomic.Value

	conn              atomic.Value
	accepted          atomic.Value
	handshakeComplete atomic.Value
	dialTimeout       atomic.Value

	clientID bitcoin.Hash20

	// Session
	hash             bitcoin.Hash32    // for generating session key
	serverSessionKey bitcoin.PublicKey // for this session
	sessionKey       bitcoin.Key
	sessionLock      sync.Mutex

	handlerChannel chan *Message

	handlers    []Handler
	handlerLock sync.Mutex

	closed atomic.Value
}

type request struct {
	typ    uint64
	hash   bitcoin.Hash32
	height int

	id       uint64
	response chan *Message
}

type requestResponse struct {
	message  *Message
	response chan<- error
}

type sendMessageRequest struct {
	msg      *Message
	response chan<- error
}

type retryConfig struct {
	maxRetries int
	retryDelay time.Duration
	errorDelay time.Duration
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
		clientID:               *clientID,
		addRequestsChannel:     make(chan *request, 100),
		removeRequestsChannel:  make(chan *request, 100),
		requestResponseChannel: make(chan *requestResponse, 100),
	}

	result.config.Store(*config)
	result.requestTimeout.Store(config.RequestTimeout.Duration)
	result.handshakeTimeout.Store(config.HandshakeTimeout.Duration)
	result.messageTimeout.Store(config.MessageChannelTimeout.Duration)
	result.dialTimeout.Store(config.DialTimeout.Duration)
	result.retryConfig.Store(retryConfig{
		maxRetries: config.MaxRetries,
		retryDelay: config.RetryDelay.Duration,
		errorDelay: config.RetryError.Duration,
	})
	result.nextMessageID.Store(uint64(1))
	result.accepted.Store(false)
	result.handshakeComplete.Store(false)
	result.closed.Store(false)

	return result, nil
}

// SetupRetry sets the maximum connection retry attempts and delay before failing.
// This can also be set from the config.
func (c *RemoteClient) SetupRetry(max int, delay time.Duration) {
	current := c.retryConfig.Load().(retryConfig)
	current.maxRetries = max
	current.retryDelay = delay
	c.retryConfig.Store(current)
}

func (c *RemoteClient) RequestTimeout() time.Duration {
	return c.requestTimeout.Load().(time.Duration)
}

func (c *RemoteClient) MessageTimeout() time.Duration {
	return c.messageTimeout.Load().(time.Duration)
}

func (c *RemoteClient) HandshakeTimeout() time.Duration {
	return c.handshakeTimeout.Load().(time.Duration)
}

func (c *RemoteClient) RetryConfig() retryConfig {
	return c.retryConfig.Load().(retryConfig)
}

func (c *RemoteClient) RegisterHandler(h Handler) {
	c.handlerLock.Lock()
	c.handlers = append(c.handlers, h)
	c.handlerLock.Unlock()
}

func (c *RemoteClient) IsAccepted(ctx context.Context) bool {
	return c.accepted.Load().(bool)
}

// SubscribePushDatas subscribes to transactions containing the specified push datas.
func (c *RemoteClient) SubscribePushDatas(ctx context.Context, pushDatas [][]byte) error {
	m := &SubscribePushData{
		PushDatas: pushDatas,
	}

	logger.Info(ctx, "Sending subscribe push data message")
	return c.sendMessage(ctx, &Message{Payload: m}, c.MessageTimeout())
}

// UnsubscribePushDatas unsubscribes to transactions containing the specified push datas.
func (c *RemoteClient) UnsubscribePushDatas(ctx context.Context, pushDatas [][]byte) error {
	m := &UnsubscribePushData{
		PushDatas: pushDatas,
	}

	logger.Info(ctx, "Sending unsubscribe push data message")
	return c.sendMessage(ctx, &Message{Payload: m}, c.MessageTimeout())
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
	return c.sendMessage(ctx, &Message{Payload: m}, c.MessageTimeout())
}

// UnsubscribeTx unsubscribes to information for a specific transaction.
func (c *RemoteClient) UnsubscribeTx(ctx context.Context, txid bitcoin.Hash32,
	indexes []uint32) error {

	m := &UnsubscribeTx{
		TxID:    txid,
		Indexes: indexes,
	}

	logger.Info(ctx, "Sending unsubscribe tx message")
	return c.sendMessage(ctx, &Message{Payload: m}, c.MessageTimeout())
}

func (c *RemoteClient) SubscribeOutputs(ctx context.Context, outputs []*wire.OutPoint) error {
	m := &SubscribeOutputs{
		Outputs: outputs,
	}

	logger.Info(ctx, "Sending subscribe outputs message")
	return c.sendMessage(ctx, &Message{Payload: m}, c.MessageTimeout())
}

func (c *RemoteClient) UnsubscribeOutputs(ctx context.Context, outputs []*wire.OutPoint) error {
	m := &UnsubscribeOutputs{
		Outputs: outputs,
	}

	logger.Info(ctx, "Sending unsubscribe outputs message")
	return c.sendMessage(ctx, &Message{Payload: m}, c.MessageTimeout())
}

// SubscribeHeaders subscribes to information on new block headers.
func (c *RemoteClient) SubscribeHeaders(ctx context.Context) error {
	m := &SubscribeHeaders{}

	logger.Info(ctx, "Sending subscribe headers message")
	return c.sendMessage(ctx, &Message{Payload: m}, c.MessageTimeout())
}

// UnsubscribeHeaders unsubscribes to information on new block headers.
func (c *RemoteClient) UnsubscribeHeaders(ctx context.Context) error {
	m := &UnsubscribeHeaders{}

	logger.Info(ctx, "Sending unsubscribe headers message")
	return c.sendMessage(ctx, &Message{Payload: m}, c.MessageTimeout())
}

// SubscribeContracts subscribes to information on contracts.
func (c *RemoteClient) SubscribeContracts(ctx context.Context) error {
	m := &SubscribeContracts{}

	logger.Info(ctx, "Sending subscribe contracts message")
	return c.sendMessage(ctx, &Message{Payload: m}, c.MessageTimeout())
}

// UnsubscribeContracts unsubscribes to information on contracts.
func (c *RemoteClient) UnsubscribeContracts(ctx context.Context) error {
	m := &UnsubscribeContracts{}

	logger.Info(ctx, "Sending unsubscribe contracts message")
	return c.sendMessage(ctx, &Message{Payload: m}, c.MessageTimeout())
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

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("next_message_id", nextMessageID),
	}, "Sending ready message")
	if err := c.sendDirect(ctx, &Message{Payload: m}); err != nil {
		return err
	}

	c.nextMessageID.Store(nextMessageID)
	c.handshakeComplete.Store(true)
	handshakeCompleteChannel := c.handshakeCompleteChannel.Load()
	if handshakeCompleteChannel != nil {
		select {
		case handshakeCompleteChannel.(chan interface{}) <- nil:
		default:
		}
	}
	return nil
}

func (c *RemoteClient) NextMessageID() uint64 {
	return c.nextMessageID.Load().(uint64)
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

	messageTimeout := c.MessageTimeout()

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
	if err := c.addRequest(request, messageTimeout); err != nil {
		return err
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Stringer("send_txid", txid),
	}, "Sending send tx request")
	m := &SendTx{
		Tx:      tx,
		Indexes: indexes,
	}
	if err := c.sendMessage(ctx, &Message{Payload: m}, messageTimeout); err != nil {
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

	case <-time.After(c.RequestTimeout()):
		if err := c.removeRequest(request, messageTimeout); err != nil {
			return err
		}

		logger.WarnWithFields(ctx, []logger.Field{
			logger.Uint64("request_id", requestID),
			logger.Stringer("send_txid", txid),
			logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		}, "Timed out waiting for send tx request")
		return ErrTimeout
	}
}

func (c *RemoteClient) SendExpandedTxAndMarkOutputs(ctx context.Context,
	etx *expanded_tx.ExpandedTx, indexes []uint32) error {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.SendExpandedTxAndMarkOutputs")

	messageTimeout := c.MessageTimeout()

	// Create request
	txid := *etx.Tx.TxHash()
	requestID := rand.Uint64()
	responseChannel := make(chan *Message, 1) // use buffer of 1 to prevent lock on write
	request := &request{
		typ:      MessageTypeSendExpandedTx,
		hash:     txid,
		id:       requestID,
		response: responseChannel,
	}

	// Add to requests so when the response is seen it can be matched up.
	if err := c.addRequest(request, messageTimeout); err != nil {
		return err
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Stringer("send_txid", txid),
	}, "Sending send expanded tx request")
	m := &SendExpandedTx{
		Tx:      etx,
		Indexes: indexes,
	}
	if err := c.sendMessage(ctx, &Message{Payload: m}, messageTimeout); err != nil {
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
			}, "Received reject for send expanded tx request : %s", rejectErr)
			return rejectErr

		case *Accept:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Uint64("request_id", requestID),
				logger.Stringer("send_txid", txid),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Received accept for send expanded tx request")
			return nil

		default:
			return fmt.Errorf("Unknown response : %d", response.Payload.Type())
		}

	case <-time.After(c.RequestTimeout()):
		if err := c.removeRequest(request, messageTimeout); err != nil {
			return err
		}

		logger.WarnWithFields(ctx, []logger.Field{
			logger.Uint64("request_id", requestID),
			logger.Stringer("send_txid", txid),
			logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		}, "Timed out waiting for send expanded tx request")
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
	if err := c.sendMessage(ctx, &Message{Payload: m}, c.RequestTimeout()); err != nil {
		return err
	}

	return nil
}

// GetTx requests a tx from the bitcoin network. It is synchronous meaning it will wait for a
// response before returning.
func (c *RemoteClient) GetTx(ctx context.Context, txid bitcoin.Hash32) (*wire.MsgTx, error) {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.GetTx")

	messageTimeout := c.MessageTimeout()

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
	if err := c.addRequest(request, messageTimeout); err != nil {
		return nil, err
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Stringer("get_txid", txid),
	}, "Sending get tx request")
	m := &GetTx{TxID: txid}
	if err := c.sendMessage(ctx, &Message{Payload: m}, messageTimeout); err != nil {
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

	case <-time.After(c.RequestTimeout()):
		if err := c.removeRequest(request, messageTimeout); err != nil {
			return nil, err
		}

		logger.WarnWithFields(ctx, []logger.Field{
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

	messageTimeout := c.MessageTimeout()

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
	if err := c.addRequest(request, messageTimeout); err != nil {
		return nil, err
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Int("request_height", height),
		logger.Int("max_count", count),
	}, "Sending get headers message")
	m := &GetHeaders{
		RequestHeight: int32(height),
		MaxCount:      uint32(count),
	}
	if err := c.sendMessage(ctx, &Message{Payload: m}, messageTimeout); err != nil {
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

	case <-time.After(c.RequestTimeout()):
		if err := c.removeRequest(request, messageTimeout); err != nil {
			return nil, err
		}

		logger.WarnWithFields(ctx, []logger.Field{
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

	messageTimeout := c.MessageTimeout()

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
	if err := c.addRequest(request, messageTimeout); err != nil {
		return nil, err
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Stringer("block_hash", blockHash),
	}, "Sending get header message")
	m := &GetHeader{
		BlockHash: blockHash,
	}
	if err := c.sendMessage(ctx, &Message{Payload: m}, messageTimeout); err != nil {
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

	case <-time.After(c.RequestTimeout()):
		if err := c.removeRequest(request, messageTimeout); err != nil {
			return nil, err
		}

		logger.WarnWithFields(ctx, []logger.Field{
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

	messageTimeout := c.MessageTimeout()

	// Create request
	requestID := rand.Uint64()
	responseChannel := make(chan *Message, 1) // use buffer of 1 to prevent lock on write
	request := &request{
		typ:      MessageTypeGetFeeQuotes,
		id:       requestID,
		response: responseChannel,
	}

	// Add to requests so when the response is seen it can be matched up.
	if err := c.addRequest(request, messageTimeout); err != nil {
		return nil, err
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
	}, "Sending get fee quotes message")
	m := &GetFeeQuotes{}
	if err := c.sendMessage(ctx, &Message{Payload: m}, messageTimeout); err != nil {
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

	case <-time.After(c.RequestTimeout()):
		if err := c.removeRequest(request, messageTimeout); err != nil {
			return nil, err
		}

		logger.WarnWithFields(ctx, []logger.Field{
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

	messageTimeout := c.MessageTimeout()

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
	if err := c.addRequest(request, messageTimeout); err != nil {
		return err
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Stringer("reprocess_txid", txid),
	}, "Sending reprocess tx request")
	m := &ReprocessTx{
		TxID:      txid,
		ClientIDs: clientIDs,
	}
	if err := c.sendMessage(ctx, &Message{Payload: m}, messageTimeout); err != nil {
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

	case <-time.After(c.RequestTimeout()):
		if err := c.removeRequest(request, messageTimeout); err != nil {
			return err
		}

		logger.WarnWithFields(ctx, []logger.Field{
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

	messageTimeout := c.MessageTimeout()

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
	if err := c.addRequest(request, messageTimeout); err != nil {
		return err
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Stringer("block_hash", blockHash),
	}, "Sending mark header invalid request")
	m := &MarkHeaderInvalid{
		BlockHash: blockHash,
	}
	if err := c.sendMessage(ctx, &Message{Payload: m}, messageTimeout); err != nil {
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

	case <-time.After(c.RequestTimeout()):
		if err := c.removeRequest(request, messageTimeout); err != nil {
			return err
		}

		logger.WarnWithFields(ctx, []logger.Field{
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

	messageTimeout := c.MessageTimeout()

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
	if err := c.addRequest(request, messageTimeout); err != nil {
		return err
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("request_id", requestID),
		logger.Stringer("block_hash", blockHash),
	}, "Sending mark header not invalid request")
	m := &MarkHeaderNotInvalid{
		BlockHash: blockHash,
	}
	if err := c.sendMessage(ctx, &Message{Payload: m}, messageTimeout); err != nil {
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

	case <-time.After(c.RequestTimeout()):
		if err := c.removeRequest(request, messageTimeout); err != nil {
			return err
		}

		logger.WarnWithFields(ctx, []logger.Field{
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
			if err := c.sendMessage(ctx, &Message{Payload: m}, c.MessageTimeout()); err != nil {
				cause := errors.Cause(err)
				if cause == ErrConnectionClosed || cause == ErrTimeout {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Float64("timestamp", float64(timeStamp)/1000000000.0),
					}, "Failed to send ping : %s", err)
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

	config := c.config.Load().(Config)
	sessionHash, err := c.generateSession(config)
	if err != nil {
		return nil, errors.Wrap(err, "session")
	}

	dialer := &net.Dialer{
		Timeout: c.dialTimeout.Load().(time.Duration),
	}
	conn, err := dialer.DialContext(ctx, "tcp", config.ServerAddress)
	if err != nil {
		return nil, errors.Wrap(err, "dial")
	}

	// Create and sign register message
	register := &Register{
		Version:          RemoteClientVersion,
		Key:              config.ClientKey.PublicKey(),
		Hash:             *sessionHash,
		StartBlockHeight: config.StartBlockHeight,
		ConnectionType:   config.ConnectionType,
	}

	sigHash, err := register.SigHash()
	if err != nil {
		conn.Close()
		return nil, errors.Wrap(err, "sig hash")
	}

	register.Signature, err = config.ClientKey.Sign(*sigHash)
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
func (c *RemoteClient) generateSession(config Config) (*bitcoin.Hash32, error) {
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
		c.serverSessionKey, err = bitcoin.NextPublicKey(config.ServerKey, c.hash)
		if err != nil {
			if errors.Cause(err) == bitcoin.ErrOutOfRangeKey {
				continue // try with a new hash
			}
			return nil, errors.Wrap(err, "next public key")
		}

		c.sessionKey, err = bitcoin.NextKey(config.ClientKey, c.hash)
		if err != nil {
			if errors.Cause(err) == bitcoin.ErrOutOfRangeKey {
				continue // try with a new hash
			}
			return nil, errors.Wrap(err, "next key")
		}

		return &c.hash, nil
	}
}

func (c *RemoteClient) sendMessage(ctx context.Context, msg *Message, timeout time.Duration) error {
	if !c.handshakeComplete.Load().(bool) && IsHandshakeType(msg.Payload.Type()) {
		// If the handshake is not complete and this is a handshake related message then it should
		// not be queued behind previously queued messages. It should be sent now.
		if err := c.sendDirect(ctx, msg); err != nil {
			return errors.Wrap(err, "send direct")
		}

		return nil
	}

	response := make(chan error, 1)
	select {
	case c.sendChannel <- &sendMessageRequest{
		msg:      msg,
		response: response,
	}:
	case <-time.After(timeout):
		logger.ErrorWithFields(ctx, []logger.Field{
			logger.String("message", NameForMessageType(msg.Payload.Type())),
		}, "Timed out adding message to send channel")
		return errors.Wrap(ErrTimeout, "add to send channel")
	}

	select {
	case err := <-response:
		return err
	case <-time.After(timeout):
		logger.ErrorWithFields(ctx, []logger.Field{
			logger.String("message", NameForMessageType(msg.Payload.Type())),
		}, "Send message timed out")
		return errors.Wrap(ErrTimeout, "send complete")
	}
}

func (c *RemoteClient) sendDirect(ctx context.Context, msg *Message) error {
	connl := c.conn.Load()
	if connl == nil {
		return ErrConnectionClosed
	}
	conn := connl.(net.Conn)

	if err := msg.Serialize(conn); err != nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.String("message", NameForMessageType(msg.Payload.Type())),
		}, "Failed to send message : %s", err)
		return err
	}

	return nil
}

func (c *RemoteClient) maintainConnection(ctx context.Context,
	sendChannel <-chan *sendMessageRequest, receiveChannel chan<- *Message,
	interrupt <-chan interface{}) error {

	first := true
	var lastConnection *time.Time
	started := time.Now()
	var messageToSend *sendMessageRequest
	retryConfig := c.RetryConfig()
	for {
		if first {
			first = false
		} else {
			if lastConnection != nil {
				since := time.Since(*lastConnection)
				if since > retryConfig.errorDelay {
					logger.ErrorWithFields(ctx, []logger.Field{
						logger.MillisecondsFromNano("delay", since.Nanoseconds()),
						logger.Formatter("seconds_since_connection", "%d", int(since.Seconds())),
					}, "Failed to connect to spynode service")
				}
			} else {
				since := time.Since(started)
				if since > retryConfig.errorDelay {
					logger.ErrorWithFields(ctx, []logger.Field{
						logger.MillisecondsFromNano("delay", since.Nanoseconds()),
						logger.Formatter("seconds_since_start", "%d", int(since.Seconds())),
					}, "Failed to connect to spynode service")
				}
			}

			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("delay", retryConfig.retryDelay),
			}, "Delaying before retrying connection")

			select {
			case <-interrupt:
				return threads.Interrupted
			case <-time.After(retryConfig.retryDelay):
			}
		}

		conn, err := c.connect(ctx)
		if err != nil {
			if lastConnection != nil {
				since := time.Since(*lastConnection)
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("client_id", c.clientID),
					logger.Formatter("seconds_since_connection", "%d", int(since.Seconds())),
				}, "Could not connect to spynode service : %s", err)
			} else {
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("client_id", c.clientID),
				}, "Could not connect to spynode service : %s", err)
			}
			continue
		}

		c.conn.Store(conn)

		messageToSend, err = c.runConnection(ctx, conn, sendChannel, receiveChannel, messageToSend,
			interrupt)
		if err != nil {
			return err
		}

		t := time.Now()
		lastConnection = &t
	}
}

func (c *RemoteClient) runConnection(ctx context.Context, conn net.Conn,
	sendChannel <-chan *sendMessageRequest, receiveChannel chan<- *Message,
	messageToSend *sendMessageRequest, interrupt <-chan interface{}) (*sendMessageRequest, error) {

	var wait sync.WaitGroup

	handshakeCompleteChannel := make(chan interface{}, 5)
	c.handshakeCompleteChannel.Store(handshakeCompleteChannel)

	sendsThread, sendsComplete := threads.NewInterruptableThreadComplete("SpyNode Sends",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			var err error
			messageToSend, err = sendMessages(ctx, conn, handshakeCompleteChannel, interrupt,
				sendChannel, c.HandshakeTimeout(), messageToSend)
			return err
		}, &wait)

	receiveThread, receiveComplete := threads.NewUninterruptableThreadComplete("SpyNode Receive",
		func(ctx context.Context) error {
			return receiveMessages(ctx, conn, receiveChannel, c.MessageTimeout())
		}, &wait)

	receiveThread.Start(ctx)
	sendsThread.Start(ctx)

	wasInterrupted := false
	var returnErr error
	select {
	case receiveErr := <-receiveComplete:
		if receiveErr != nil {
			if isClosedError(receiveErr) {
				logger.Info(ctx, "Disconnected")
			} else {
				if _, ok := errors.Cause(receiveErr).(RejectError); ok {
					logger.Error(ctx, "Connection rejected : %s", receiveErr)
					returnErr = receiveErr
				} else {
					logger.Warn(ctx, "Receive messages failed : %s", receiveErr)
				}
			}
		} else {
			logger.Warn(ctx, "Receive messages completed")
		}

	case sendErr := <-sendsComplete:
		if sendErr != nil {
			if isClosedError(sendErr) {
				logger.Info(ctx, "Disconnected")
			} else {
				if _, ok := errors.Cause(sendErr).(RejectError); ok {
					logger.Error(ctx, "Connection rejected : %s", sendErr)
					returnErr = sendErr
				} else {
					logger.Warn(ctx, "Send messages failed : %s", sendErr)
				}
			}
		} else {
			logger.Warn(ctx, "Send messages completed")
		}

	case <-interrupt:
		logger.Info(ctx, "Connection shut down")
		wasInterrupted = true
	}

	sendsThread.Stop(ctx)
	select {
	case handshakeCompleteChannel <- nil: // ensure sendMessages is not waiting on the handshake
	default:
	}
	conn.Close()

	wait.Wait()

	if returnErr != nil {
		return nil, returnErr
	}
	if wasInterrupted {
		return nil, threads.Interrupted
	}

	return messageToSend, nil
}

func isClosedError(err error) bool {
	cause := errors.Cause(err)
	if cause == io.EOF || cause == io.ErrUnexpectedEOF {
		return true
	}

	s := err.Error()
	if strings.Contains(s, "Closed") || strings.Contains(s, "use of closed network connection") {
		return true
	}

	return false
}

func sendMessages(ctx context.Context, conn net.Conn,
	handshakeComplete, interrupt <-chan interface{}, sendChannel <-chan *sendMessageRequest,
	timeout time.Duration, firstMsg *sendMessageRequest) (*sendMessageRequest, error) {

	select {
	case <-handshakeComplete:
	case <-time.After(timeout):
		return firstMsg, errors.Wrap(ErrTimeout, "handshake")
	}

	if firstMsg != nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.String("message", NameForMessageType(firstMsg.msg.Payload.Type())),
		}, "Re-sending message")

		if err := firstMsg.msg.Serialize(conn); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.String("message", NameForMessageType(firstMsg.msg.Payload.Type())),
			}, "Failed to send message : %s", err)
			return firstMsg, err
		}

		if firstMsg.response != nil {
			firstMsg.response <- nil
		}
	}

	for {
		select {
		case <-interrupt:
			return nil, nil
		case msg := <-sendChannel:
			if err := msg.msg.Serialize(conn); err != nil {
				logger.WarnWithFields(ctx, []logger.Field{
					logger.String("message", NameForMessageType(msg.msg.Payload.Type())),
				}, "Failed to send message : %s", err)
				return msg, err
			}

			if msg.response != nil {
				msg.response <- nil
			}
		}
	}
}

func receiveMessages(ctx context.Context, conn net.Conn, receiveChannel chan<- *Message,
	timeout time.Duration) error {

	for {
		message := &Message{}
		if err := message.Deserialize(conn); err != nil {
			return err
		}

		select {
		case receiveChannel <- message:
		case <-time.After(timeout):
			return errors.Wrap(ErrTimeout, "add receive channel")
		}
	}
}

func (c *RemoteClient) Run(ctx context.Context, interrupt <-chan interface{}) error {
	clientID := c.clientID.Copy()

	defer func() {
		c.closed.Store(true)
	}()

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("client_id", clientID))
	logger.Info(ctx, "Starting spynode remote client")
	defer logger.Info(ctx, "Spynode remote client completed")

	receiveChannel := make(chan *Message, 100)
	c.sendChannel = make(chan *sendMessageRequest, 100)
	c.handlerChannel = make(chan *Message, 100)

	var wait, connectionWait sync.WaitGroup
	var stopper threads.StopCombiner

	handleMessagesThread, handleMessagesComplete := threads.NewInterruptableThreadComplete("SpyNode Handle Messages",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return c.handleMessages(ctx, receiveChannel, interrupt)
		}, &wait)
	stopper.Add(handleMessagesThread)

	handlerThread, handlerComplete := threads.NewInterruptableThreadComplete("SpyNode Handler",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return c.runHandler(ctx, c.handlerChannel, interrupt)
		}, &wait)
	stopper.Add(handlerThread)

	requestsThread, requestsComplete := threads.NewInterruptableThreadComplete("SpyNode Requests",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return c.runRequests(ctx, interrupt)
		}, &wait)
	stopper.Add(requestsThread)

	pingThread, pingComplete := threads.NewInterruptableThreadComplete("SpyNode Ping", c.ping,
		&wait)
	stopper.Add(pingThread)

	connectionThread, connectionComplete := threads.NewInterruptableThreadComplete("SpyNode Connection",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return c.maintainConnection(ctx, c.sendChannel, receiveChannel, interrupt)
		}, &connectionWait)
	stopper.Add(connectionThread)

	handleMessagesThread.Start(ctx)
	handlerThread.Start(ctx)
	requestsThread.Start(ctx)
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

	case requestsErr := <-requestsComplete:
		if requestsErr != nil {
			logger.Warn(ctx, "Requests thread failed : %s", requestsErr)
		} else {
			logger.Warn(ctx, "Requests thread completed")
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
		logger.Info(ctx, "SpyNode remote client shutdown requested")
	}

	stopper.Stop(ctx)

	connectionWait.Wait()

	wait.Wait()

	return threads.CombineErrors(
		handleMessagesThread.Error(),
		handlerThread.Error(),
		pingThread.Error(),
		connectionThread.Error(),
	)
}

func (c *RemoteClient) addRequest(request *request, timeout time.Duration) error {
	select {
	case c.addRequestsChannel <- request:
		return nil
	case <-time.After(timeout):
		return errors.Wrap(ErrTimeout, "add request")
	}
}

func (c *RemoteClient) addRequestResponse(response *requestResponse, timeout time.Duration) error {

	select {
	case c.requestResponseChannel <- response:
		return nil
	case <-time.After(timeout):
		return errors.Wrap(ErrTimeout, "add request response")
	}
}

func (c *RemoteClient) removeRequest(request *request, timeout time.Duration) error {
	select {
	case c.removeRequestsChannel <- request:
		return nil
	case <-time.After(timeout):
		return errors.Wrap(ErrTimeout, "remove request")
	}
}

func (c *RemoteClient) runRequests(ctx context.Context, interrupt <-chan interface{}) error {
	for {
		select {
		case <-interrupt:
			return nil

		case request := <-c.addRequestsChannel:
			c.requests = append(c.requests, request)

		case request := <-c.removeRequestsChannel:
			for i, r := range c.requests {
				if r == request {
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					break
				}
			}

		case response := <-c.requestResponseChannel:
			err := c.handleRequestResponse(ctx, response.message)
			if response.response != nil {
				response.response <- err
			} else if err != nil {
				logger.WarnWithFields(ctx, []logger.Field{
					logger.String("name", NameForMessageType(response.message.Payload.Type())),
				}, "Failed to handle request response : %s", err)
			}
		}
	}
}

func (c *RemoteClient) handleRequestResponse(ctx context.Context, message *Message) error {
	switch msg := message.Payload.(type) {
	case *Headers:
		for i, request := range c.requests {
			if request.typ == MessageTypeGetHeaders && request.height == int(msg.RequestHeight) {
				request.response <- message
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				return nil
			}
		}

		return ErrRequestNotFound

	case *Header:
		blockHash := *msg.Header.BlockHash()
		for i, request := range c.requests {
			if request.typ == MessageTypeGetHeaders && request.hash.Equal(&blockHash) {
				request.response <- message
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				return nil
			}
		}

	case *FeeQuotes:
		for i, request := range c.requests {
			if request.typ == MessageTypeGetFeeQuotes {
				request.response <- message
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				return nil
			}
		}

	case *BaseTx:
		txid := *msg.Tx.TxHash()
		for i, request := range c.requests {
			if request.typ == MessageTypeGetTx && request.hash.Equal(&txid) {
				request.response <- message
				c.requests = append(c.requests[:i], c.requests[i+1:]...)
				return nil
			}
		}

		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txid),
		}, "No matching request found for base tx")

	case *Accept:
		if msg.Hash == nil {
			logger.Warn(ctx, "Received accept with no hash")
			return nil
		}

		switch msg.MessageType {
		case MessageTypeSendTx:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "Received accept for send tx")

			for i, request := range c.requests {
				if request.typ == MessageTypeSendTx && request.hash.Equal(msg.Hash) {
					request.response <- message
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					return nil
				}
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "No matching request found for send tx accept")

		case MessageTypeSendExpandedTx:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "Received accept for send expanded tx")

			for i, request := range c.requests {
				if request.typ == MessageTypeSendExpandedTx && request.hash.Equal(msg.Hash) {
					request.response <- message
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					return nil
				}
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "No matching request found for send expanded tx accept")

		case MessageTypeReprocessTx:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "Received accept for reprocess tx")

			for i, request := range c.requests {
				if request.typ == MessageTypeReprocessTx && request.hash.Equal(msg.Hash) {
					request.response <- message
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					return nil
				}
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "No matching request found for mark header invalid accept")

		case MessageTypeMarkHeaderInvalid:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", msg.Hash),
			}, "Received accept for mark header invalid")

			for i, request := range c.requests {
				if request.typ == MessageTypeMarkHeaderInvalid && request.hash.Equal(msg.Hash) {
					request.response <- message
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					return nil
				}
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", msg.Hash),
			}, "No matching request found for mark header invalid accept")

		case MessageTypeMarkHeaderNotInvalid:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", msg.Hash),
			}, "Received accept for mark header not invalid")

			for i, request := range c.requests {
				if request.typ == MessageTypeMarkHeaderNotInvalid && request.hash.Equal(msg.Hash) {
					request.response <- message
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					return nil
				}
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", msg.Hash),
			}, "No matching request found for mark header not invalid accept")

		default:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Uint64("type", msg.MessageType),
				logger.String("name", NameForMessageType(msg.MessageType)),
				logger.Stringer("hash", msg.Hash),
			}, "Received accept for unsupported message")

		}

	case *Reject:
		if msg.Hash == nil {
			logger.Info(ctx, "Received reject with no hash")
			return nil
		}

		switch msg.MessageType {
		case MessageTypeSendTx:
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "Received reject for send tx : %s", msg.Message)

			for i, request := range c.requests {
				if request.typ == MessageTypeSendTx && request.hash.Equal(msg.Hash) {
					request.response <- message
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					return nil
				}
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "No matching request found for send tx reject")

		case MessageTypeSendExpandedTx:
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "Received reject for send expanded tx : %s", msg.Message)

			for i, request := range c.requests {
				if request.typ == MessageTypeSendExpandedTx && request.hash.Equal(msg.Hash) {
					request.response <- message
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					return nil
				}
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "No matching request found for send expanded tx reject")

		case MessageTypeGetTx:
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "Received reject for get tx : %s", msg.Message)

			for i, request := range c.requests {
				if request.typ == MessageTypeGetTx && request.hash.Equal(msg.Hash) {
					request.response <- message
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					return nil
				}
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "No matching request found for get tx reject")

		case MessageTypeGetHeader:
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", msg.Hash),
			}, "Received reject for get header : %s", msg.Message)

			for i, request := range c.requests {
				if request.typ == MessageTypeGetHeader && request.hash.Equal(msg.Hash) {
					request.response <- message
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					return nil
				}
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "No matching request found for get header reject")

		case MessageTypeGetFeeQuotes:
			logger.Warn(ctx, "Received reject for get fee quotes : %s", msg.Message)

			for i, request := range c.requests {
				if request.typ == MessageTypeGetFeeQuotes {
					request.response <- message
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					return nil
				}
			}

		case MessageTypeReprocessTx:
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "Received reject for reprocess tx : %s", msg.Message)

			for i, request := range c.requests {
				if request.typ == MessageTypeReprocessTx && request.hash.Equal(msg.Hash) {
					request.response <- message
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					return nil
				}
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", msg.Hash),
			}, "No matching request found for reprocess tx reject")

		case MessageTypeMarkHeaderInvalid:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", msg.Hash),
			}, "Received reject for mark header invalid")

			for i, request := range c.requests {
				if request.typ == MessageTypeMarkHeaderInvalid && request.hash.Equal(msg.Hash) {
					request.response <- message
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					return nil
				}
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", msg.Hash),
			}, "No matching request found for mark header invalid reject")

		case MessageTypeMarkHeaderNotInvalid:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", msg.Hash),
			}, "Received reject for mark header not invalid")

			for i, request := range c.requests {
				if request.typ == MessageTypeMarkHeaderNotInvalid && request.hash.Equal(msg.Hash) {
					request.response <- message
					c.requests = append(c.requests[:i], c.requests[i+1:]...)
					return nil
				}
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", msg.Hash),
			}, "No matching request found for mark header not invalid reject")

		default:
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Uint64("type", msg.MessageType),
				logger.String("name", NameForMessageType(msg.MessageType)),
				logger.Stringer("hash", msg.Hash),
			}, "Received reject for unsupported message")
		}

	default:
		logger.InfoWithFields(ctx, []logger.Field{
			logger.String("name", NameForMessageType(msg.Type())),
		}, "Received response for unsupported message type")
	}

	return nil
}

func (c *RemoteClient) addHandlerMessage(ctx context.Context, msg *Message) error {
	select {
	case c.handlerChannel <- msg:
		return nil
	case <-time.After(c.MessageTimeout()):
		return errors.Wrap(ErrTimeout, "handler channel")
	}
}

func (c *RemoteClient) handleMessages(ctx context.Context, receiveChannel <-chan *Message,
	interrupt <-chan interface{}) error {

	for {
		select {
		case msg := <-receiveChannel:
			if err := c.handleMessage(ctx, msg); err != nil {
				return err
			}
		case <-interrupt:
			return nil
		}
	}
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
		c.accepted.Store(true)

		// When in full mode wait for the client to call the Ready function, but when not just mark
		// the handshake as complete.
		config := c.config.Load().(Config)
		if config.ConnectionType != ConnectionTypeFull {
			c.handshakeComplete.Store(true)
			handshakeCompleteChannel := c.handshakeCompleteChannel.Load()
			if handshakeCompleteChannel != nil {
				select {
				case handshakeCompleteChannel.(chan interface{}) <- nil:
				default:
				}
			}
		}

		c.addHandlerMessage(ctx, m)

	case *Tx:
		txid := *msg.Tx.TxHash()
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txid),
			logger.Uint64("message_id", msg.ID),
		}, "Received tx")

		nextMessageID := c.nextMessageID.Load().(uint64)
		if nextMessageID != msg.ID {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Uint64("expected_message_id", nextMessageID),
				logger.Uint64("message_id", msg.ID),
			}, "Wrong message ID in tx message")
		} else {
			c.nextMessageID.Store(msg.ID + 1)
			c.addHandlerMessage(ctx, m)
		}

	case *TxUpdate:
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("txid", msg.TxID),
			logger.Uint64("message_id", msg.ID),
		}, "Received tx state")

		nextMessageID := c.nextMessageID.Load().(uint64)
		if nextMessageID != msg.ID {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Uint64("expected_message_id", nextMessageID),
				logger.Uint64("message_id", msg.ID),
			}, "Wrong message ID in tx update message")
		} else {
			c.nextMessageID.Store(msg.ID + 1)
			c.addHandlerMessage(ctx, m)
		}

	case *Headers:
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Int("header_count", len(msg.Headers)),
			logger.Uint32("start_height", msg.StartHeight),
		}, "Received headers")

		responseChannel := make(chan error, 1)
		if err := c.addRequestResponse(&requestResponse{
			message:  m,
			response: responseChannel,
		}, c.MessageTimeout()); err != nil {
			logger.Error(ctx, "Failed to add request response : %s", err)
		}

		err := <-responseChannel
		if err != nil && errors.Cause(err) == ErrRequestNotFound {
			c.addHandlerMessage(ctx, m)
		}

	case *Header:
		blockHash := *msg.Header.BlockHash()
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("block_hash", blockHash),
		}, "Received header")

		if err := c.addRequestResponse(&requestResponse{
			message:  m,
			response: nil,
		}, c.MessageTimeout()); err != nil {
			logger.Error(ctx, "Failed to add request response : %s", err)
		}

	case *FeeQuotes:
		logger.Info(ctx, "Received fee quotes")

		if err := c.addRequestResponse(&requestResponse{
			message:  m,
			response: nil,
		}, c.MessageTimeout()); err != nil {
			logger.Error(ctx, "Failed to add request response : %s", err)
		}

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

		if err := c.addRequestResponse(&requestResponse{
			message:  m,
			response: nil,
		}, c.MessageTimeout()); err != nil {
			logger.Error(ctx, "Failed to add request response : %s", err)
		}

	case *Accept:
		logger.Info(ctx, "Received accept")

		if err := c.addRequestResponse(&requestResponse{
			message:  m,
			response: nil,
		}, c.MessageTimeout()); err != nil {
			logger.Error(ctx, "Failed to add request response : %s", err)
		}

	case *Reject:
		logger.Info(ctx, "Received reject")

		if !c.accepted.Load().(bool) {
			// Service rejected registration
			logger.Info(ctx, "Reject registration")
			return NewRejectError(msg.Code, msg.Message)
		}

		if err := c.addRequestResponse(&requestResponse{
			message:  m,
			response: nil,
		}, c.MessageTimeout()); err != nil {
			logger.Error(ctx, "Failed to add request response : %s", err)
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

func (c *RemoteClient) runHandler(ctx context.Context, handlerChannel <-chan *Message,
	interrupt <-chan interface{}) error {

	for {
		select {
		case msg := <-handlerChannel:
			if err := c.processHandler(ctx, msg); err != nil {
				return err
			}
		case <-interrupt:
			return nil
		}
	}
}

func (c *RemoteClient) processHandler(ctx context.Context, msg *Message) error {
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

	return nil
}
