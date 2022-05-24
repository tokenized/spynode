package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/tokenized/config"
	"github.com/tokenized/metrics"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/merchant_api"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/wire"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var (
	Endian = binary.LittleEndian

	// RemoteClientVersion is the current version of the communication protocol
	RemoteClientVersion = uint8(0)
)

// RemoteClient is a client for interacting with the spynode service.
type RemoteClient struct {
	conn net.Conn

	config        *Config
	nextMessageID uint64

	// Session
	hash             bitcoin.Hash32    // for generating session key
	serverSessionKey bitcoin.PublicKey // for this session
	sessionKey       bitcoin.Key

	handlers    []Handler
	handlerLock sync.Mutex

	// Process handlers in separate thread to prevent conflict between listener thread calling
	// handlers directly listener thread waiting for responses from within handler.
	handlerChannel       chan MessagePayload
	handlerChannelIsOpen bool
	handlerChannelLock   sync.Mutex

	// Requests
	sendTxRequests               []*sendTxRequest
	getTxRequests                []*getTxRequest
	headersRequests              []*headersRequest
	headerRequests               []*headerRequest
	feeQuoteRequests             []*feeQuoteRequest
	reprocessTxRequests          []*reprocessTxRequest
	markHeaderInvalidRequests    []*markHeaderInvalidRequest
	markHeaderNotInvalidRequests []*markHeaderNotInvalidRequest
	requestLock                  sync.Mutex

	accepted, ready bool
	lock            sync.Mutex
	wait            sync.WaitGroup

	closeRequested     bool
	closeRequestedLock sync.Mutex

	listenErrChannel *chan error
}

type sendTxRequest struct {
	txid     bitcoin.Hash32
	response *Message
	lock     sync.Mutex
}

type getTxRequest struct {
	txid     bitcoin.Hash32
	response *Message
	lock     sync.Mutex
}

type headersRequest struct {
	height   int
	response *Message
	lock     sync.Mutex
}

type headerRequest struct {
	hash     bitcoin.Hash32
	response *Message
	lock     sync.Mutex
}

type feeQuoteRequest struct {
	response *Message
	lock     sync.Mutex
}

type reprocessTxRequest struct {
	txid     bitcoin.Hash32
	response *Message
	lock     sync.Mutex
}

type markHeaderInvalidRequest struct {
	blockHash bitcoin.Hash32
	response  *Message
	lock      sync.Mutex
}

type markHeaderNotInvalidRequest struct {
	blockHash bitcoin.Hash32
	response  *Message
	lock      sync.Mutex
}

// NewRemoteClient creates a remote client.
// Note: If the connection type is not "full" then it will auto-connect when a function is called to
// communicate with the spynode service. Make sure `Close` is called before application end so that
// the connection can be closed and the listen thread completed.
func NewRemoteClient(config *Config) (*RemoteClient, error) {
	result := &RemoteClient{
		config:        config,
		nextMessageID: 1,
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
	return c.sendMessage(ctx, m)
}

// UnsubscribePushDatas unsubscribes to transactions containing the specified push datas.
func (c *RemoteClient) UnsubscribePushDatas(ctx context.Context, pushDatas [][]byte) error {
	m := &UnsubscribePushData{
		PushDatas: pushDatas,
	}

	logger.Info(ctx, "Sending unsubscribe push data message")
	return c.sendMessage(ctx, m)
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
	return c.sendMessage(ctx, m)
}

// UnsubscribeTx unsubscribes to information for a specific transaction.
func (c *RemoteClient) UnsubscribeTx(ctx context.Context, txid bitcoin.Hash32,
	indexes []uint32) error {

	m := &UnsubscribeTx{
		TxID:    txid,
		Indexes: indexes,
	}

	logger.Info(ctx, "Sending unsubscribe tx message")
	return c.sendMessage(ctx, m)
}

func (c *RemoteClient) SubscribeOutputs(ctx context.Context, outputs []*wire.OutPoint) error {
	m := &SubscribeOutputs{
		Outputs: outputs,
	}

	logger.Info(ctx, "Sending subscribe outputs message")
	return c.sendMessage(ctx, m)
}

func (c *RemoteClient) UnsubscribeOutputs(ctx context.Context, outputs []*wire.OutPoint) error {
	m := &UnsubscribeOutputs{
		Outputs: outputs,
	}

	logger.Info(ctx, "Sending unsubscribe outputs message")
	return c.sendMessage(ctx, m)
}

// SubscribeHeaders subscribes to information on new block headers.
func (c *RemoteClient) SubscribeHeaders(ctx context.Context) error {
	m := &SubscribeHeaders{}

	logger.Info(ctx, "Sending subscribe headers message")
	return c.sendMessage(ctx, m)
}

// UnsubscribeHeaders unsubscribes to information on new block headers.
func (c *RemoteClient) UnsubscribeHeaders(ctx context.Context) error {
	m := &UnsubscribeHeaders{}

	logger.Info(ctx, "Sending unsubscribe headers message")
	return c.sendMessage(ctx, m)
}

// SubscribeContracts subscribes to information on contracts.
func (c *RemoteClient) SubscribeContracts(ctx context.Context) error {
	m := &SubscribeContracts{}

	logger.Info(ctx, "Sending subscribe contracts message")
	return c.sendMessage(ctx, m)
}

// UnsubscribeContracts unsubscribes to information on contracts.
func (c *RemoteClient) UnsubscribeContracts(ctx context.Context) error {
	m := &UnsubscribeContracts{}

	logger.Info(ctx, "Sending unsubscribe contracts message")
	return c.sendMessage(ctx, m)
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
	if err := c.sendMessage(ctx, m); err != nil {
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

	// Register with listener for response
	request := &sendTxRequest{
		txid: *tx.TxHash(),
	}

	c.requestLock.Lock()
	c.sendTxRequests = append(c.sendTxRequests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("send_txid", request.txid),
	}, "Sending send tx request")
	m := &SendTx{
		Tx:      tx,
		Indexes: indexes,
	}
	if err := c.sendMessage(ctx, m); err != nil {
		return err
	}

	// Wait for response
	timeout := time.Now().Add(c.config.RequestTimeout.Duration)
	for time.Now().Before(timeout) {
		request.lock.Lock()
		done := request.response != nil
		request.lock.Unlock()

		if done {
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	// Remove from requests
	c.requestLock.Lock()
	for i, r := range c.sendTxRequests {
		if r == request {
			c.sendTxRequests = append(c.sendTxRequests[:i], c.sendTxRequests[i+1:]...)
			break
		}
	}
	c.requestLock.Unlock()

	if request.response != nil {
		switch msg := request.response.Payload.(type) {
		case *Reject:
			return NewRejectError(msg.Code, msg.Message)
		case *Accept:
			return nil
		default:
			return fmt.Errorf("Unknown response : %d", request.response.Payload.Type())
		}
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("send_txid", request.txid),
	}, "Timed out waiting for send tx request")
	return ErrTimeout
}

// GetTx requests a tx from the bitcoin network. It is synchronous meaning it will wait for a
// response before returning.
func (c *RemoteClient) GetTx(ctx context.Context, txid bitcoin.Hash32) (*wire.MsgTx, error) {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.GetTx")

	// Register with listener for response tx
	request := &getTxRequest{
		txid: txid,
	}

	c.requestLock.Lock()
	c.getTxRequests = append(c.getTxRequests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("get_txid", txid),
	}, "Sending get tx request")
	m := &GetTx{TxID: txid}
	if err := c.sendMessage(ctx, m); err != nil {
		return nil, err
	}

	// Wait for response
	timeout := time.Now().Add(c.config.RequestTimeout.Duration)
	for time.Now().Before(timeout) {
		request.lock.Lock()
		done := request.response != nil
		request.lock.Unlock()

		if done {
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	// Remove from requests
	c.requestLock.Lock()
	for i, r := range c.getTxRequests {
		if r == request {
			c.getTxRequests = append(c.getTxRequests[:i], c.getTxRequests[i+1:]...)
			break
		}
	}
	c.requestLock.Unlock()

	if request.response != nil {
		switch msg := request.response.Payload.(type) {
		case *Reject:
			return nil, NewRejectError(msg.Code, msg.Message)
		case *BaseTx:
			return msg.Tx, nil
		default:
			return nil, fmt.Errorf("Unknown response : %d", request.response.Payload.Type())
		}
	}

	logger.ErrorWithFields(ctx, []logger.Field{
		logger.Stringer("get_txid", txid),
	}, "Timed out waiting for get tx request")
	return nil, ErrTimeout
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

	// Register with listener for response tx
	request := &headersRequest{
		height: height,
	}

	c.requestLock.Lock()
	c.headersRequests = append(c.headersRequests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("height", height),
		logger.Int("count", count),
	}, "Sending get headers message")
	m := &GetHeaders{
		RequestHeight: int32(height),
		MaxCount:      uint32(count),
	}
	if err := c.sendMessage(ctx, m); err != nil {
		return nil, err
	}

	// Wait for response
	timeout := time.Now().Add(c.config.RequestTimeout.Duration)
	for time.Now().Before(timeout) {
		request.lock.Lock()
		done := request.response != nil
		request.lock.Unlock()

		if done {
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	// Remove from requests
	c.requestLock.Lock()
	for i, r := range c.headersRequests {
		if r == request {
			c.headersRequests = append(c.headersRequests[:i], c.headersRequests[i+1:]...)
			break
		}
	}
	c.requestLock.Unlock()

	if request.response != nil {
		switch msg := request.response.Payload.(type) {
		case *Reject:
			return nil, NewRejectError(msg.Code, msg.Message)
		case *Headers:
			return msg, nil
		default:
			return nil, fmt.Errorf("Unknown response : %d", request.response.Payload.Type())
		}
	}

	return nil, ErrTimeout
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

	// Register with listener for response tx
	request := &headerRequest{
		hash: blockHash,
	}

	c.requestLock.Lock()
	c.headerRequests = append(c.headerRequests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("block_hash", blockHash),
	}, "Sending get header message")
	m := &GetHeader{
		BlockHash: blockHash,
	}
	if err := c.sendMessage(ctx, m); err != nil {
		return nil, err
	}

	// Wait for response
	timeout := time.Now().Add(c.config.RequestTimeout.Duration)
	for time.Now().Before(timeout) {
		request.lock.Lock()
		done := request.response != nil
		request.lock.Unlock()

		if done {
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	// Remove from requests
	c.requestLock.Lock()
	for i, r := range c.headerRequests {
		if r == request {
			c.headerRequests = append(c.headerRequests[:i], c.headerRequests[i+1:]...)
			break
		}
	}
	c.requestLock.Unlock()

	if request.response != nil {
		switch msg := request.response.Payload.(type) {
		case *Reject:
			return nil, NewRejectError(msg.Code, msg.Message)
		case *Header:
			return msg, nil
		default:
			return nil, fmt.Errorf("Unknown response : %d", request.response.Payload.Type())
		}
	}

	return nil, ErrTimeout
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

	// Register with listener for response tx
	request := &feeQuoteRequest{}

	c.requestLock.Lock()
	c.feeQuoteRequests = append(c.feeQuoteRequests, request)
	c.requestLock.Unlock()

	logger.Info(ctx, "Sending get fee quotes message")
	m := &GetFeeQuotes{}
	if err := c.sendMessage(ctx, m); err != nil {
		return nil, err
	}

	// Wait for response
	timeout := time.Now().Add(c.config.RequestTimeout.Duration)
	for time.Now().Before(timeout) {
		request.lock.Lock()
		done := request.response != nil
		request.lock.Unlock()

		if done {
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	// Remove from requests
	c.requestLock.Lock()
	for i, r := range c.feeQuoteRequests {
		if r == request {
			c.feeQuoteRequests = append(c.feeQuoteRequests[:i], c.feeQuoteRequests[i+1:]...)
			break
		}
	}
	c.requestLock.Unlock()

	if request.response != nil {
		switch msg := request.response.Payload.(type) {
		case *Reject:
			return nil, NewRejectError(msg.Code, msg.Message)
		case *FeeQuotes:
			return msg.FeeQuotes, nil
		default:
			return nil, fmt.Errorf("Unknown response : %d", request.response.Payload.Type())
		}
	}

	return nil, ErrTimeout
}

// ReprocessTx requests that a tx be reprocessed.
func (c *RemoteClient) ReprocessTx(ctx context.Context, txid bitcoin.Hash32,
	clientIDs []bitcoin.Hash20) error {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.ReprocessTx")

	// Register with listener for response
	request := &reprocessTxRequest{
		txid: txid,
	}

	c.requestLock.Lock()
	c.reprocessTxRequests = append(c.reprocessTxRequests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("txid", request.txid),
	}, "Sending reprocess tx request")
	m := &ReprocessTx{
		TxID:      txid,
		ClientIDs: clientIDs,
	}
	if err := c.sendMessage(ctx, m); err != nil {
		return err
	}

	// Wait for response
	timeout := time.Now().Add(c.config.RequestTimeout.Duration)
	for time.Now().Before(timeout) {
		request.lock.Lock()
		done := request.response != nil
		request.lock.Unlock()

		if done {
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	// Remove from requests
	c.requestLock.Lock()
	for i, r := range c.reprocessTxRequests {
		if r == request {
			c.reprocessTxRequests = append(c.reprocessTxRequests[:i],
				c.reprocessTxRequests[i+1:]...)
			break
		}
	}
	c.requestLock.Unlock()

	if request.response != nil {
		switch msg := request.response.Payload.(type) {
		case *Reject:
			return NewRejectError(msg.Code, msg.Message)
		case *Accept:
			return nil
		default:
			return fmt.Errorf("Unknown response : %d", request.response.Payload.Type())
		}
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("txid", request.txid),
	}, "Timed out waiting for reprocess tx request")
	return ErrTimeout
}

// MarkHeaderInvalid request that a block hash is marked as invalid.
func (c *RemoteClient) MarkHeaderInvalid(ctx context.Context, blockHash bitcoin.Hash32) error {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.MarkHeaderInvalid")

	// Register with listener for response
	request := &markHeaderInvalidRequest{
		blockHash: blockHash,
	}

	c.requestLock.Lock()
	c.markHeaderInvalidRequests = append(c.markHeaderInvalidRequests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("block_hash", request.blockHash),
	}, "Sending mark header invalid request")
	m := &MarkHeaderInvalid{
		BlockHash: blockHash,
	}
	if err := c.sendMessage(ctx, m); err != nil {
		return err
	}

	// Wait for response
	timeout := time.Now().Add(c.config.RequestTimeout.Duration)
	for time.Now().Before(timeout) {
		request.lock.Lock()
		done := request.response != nil
		request.lock.Unlock()

		if done {
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	// Remove from requests
	c.requestLock.Lock()
	for i, r := range c.markHeaderInvalidRequests {
		if r == request {
			c.markHeaderInvalidRequests = append(c.markHeaderInvalidRequests[:i],
				c.markHeaderInvalidRequests[i+1:]...)
			break
		}
	}
	c.requestLock.Unlock()

	if request.response != nil {
		switch msg := request.response.Payload.(type) {
		case *Reject:
			return NewRejectError(msg.Code, msg.Message)
		case *Accept:
			return nil
		default:
			return fmt.Errorf("Unknown response : %d", request.response.Payload.Type())
		}
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("block_hash", request.blockHash),
	}, "Timed out waiting for mark header invalid request")
	return ErrTimeout
}

// MarkHeaderNotInvalid request that a block hash is marked as not invalid.
func (c *RemoteClient) MarkHeaderNotInvalid(ctx context.Context, blockHash bitcoin.Hash32) error {
	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.MarkHeaderNotInvalid")

	// Register with listener for response
	request := &markHeaderNotInvalidRequest{
		blockHash: blockHash,
	}

	c.requestLock.Lock()
	c.markHeaderNotInvalidRequests = append(c.markHeaderNotInvalidRequests, request)
	c.requestLock.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("block_hash", request.blockHash),
	}, "Sending mark header not invalid request")
	m := &MarkHeaderNotInvalid{
		BlockHash: blockHash,
	}
	if err := c.sendMessage(ctx, m); err != nil {
		return err
	}

	// Wait for response
	timeout := time.Now().Add(c.config.RequestTimeout.Duration)
	for time.Now().Before(timeout) {
		request.lock.Lock()
		done := request.response != nil
		request.lock.Unlock()

		if done {
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	// Remove from requests
	c.requestLock.Lock()
	for i, r := range c.markHeaderNotInvalidRequests {
		if r == request {
			c.markHeaderNotInvalidRequests = append(c.markHeaderNotInvalidRequests[:i],
				c.markHeaderNotInvalidRequests[i+1:]...)
			break
		}
	}
	c.requestLock.Unlock()

	if request.response != nil {
		switch msg := request.response.Payload.(type) {
		case *Reject:
			return NewRejectError(msg.Code, msg.Message)
		case *Accept:
			return nil
		default:
			return fmt.Errorf("Unknown response : %d", request.response.Payload.Type())
		}
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("block_hash", request.blockHash),
	}, "Timed out waiting for mark header not invalid request")
	return ErrTimeout
}

// sendMessage wraps and sends a message to the server.
func (c *RemoteClient) sendMessage(ctx context.Context, payload MessagePayload) error {
	if c.config.ConnectionType != ConnectionTypeFull {
		// Connect if not already connected
		if err := c.Connect(ctx); err != nil {
			return errors.Wrap(err, "connect")
		}
	}

	c.lock.Lock()

	if c.conn == nil {
		c.lock.Unlock()
		return ErrNotConnected
	}

	conn := c.conn
	c.lock.Unlock()

	message := &Message{
		Payload: payload,
	}

	// TODO Possibly add streaming encryption here. --ce

	if err := message.Serialize(conn); err != nil {
		return errors.Wrap(err, "send message")
	}

	return nil
}

func (c *RemoteClient) IsConnected() bool {
	c.lock.Lock()
	result := c.conn != nil
	c.lock.Unlock()

	return result
}

// SetListenerErrorChannel sets a channel that will receive an error when the listener returns.
func (c *RemoteClient) SetListenerErrorChannel(channel *chan error) {
	c.listenErrChannel = channel
}

// Connect connects to the spynode service if it isn't already connected and also starts the
// listiner thread.
func (c *RemoteClient) Connect(ctx context.Context) error {
	if isNewConnection, err := c.connect(ctx); err != nil {
		return err
	} else if !isNewConnection {
		return nil
	}

	threadCtx := logger.ContextWithLogTrace(ctx, uuid.New().String())

	// Start listener thread
	c.wait.Add(1)
	go func() {
		logger.Info(threadCtx, "Spynode client listening")
		if c.listenErrChannel != nil {
			*c.listenErrChannel <- c.listen(threadCtx)
		} else {
			if err := c.listen(threadCtx); err != nil {
				logger.Warn(threadCtx, "Listener finished with error : %s", err)
			}
		}
		logger.Info(threadCtx, "Spynode client finished listening")
		c.wait.Done()
	}()

	// Start handler thread
	c.handlerChannelLock.Lock()
	if c.handlerChannelIsOpen {
		logger.Info(ctx, "Handler channel already open")
	}
	c.handlerChannelIsOpen = true
	c.handlerChannel = make(chan MessagePayload, 1000)
	c.handlerChannelLock.Unlock()

	c.wait.Add(1)
	go func() {
		logger.Info(threadCtx, "Spynode client handler running")
		if err := c.handle(threadCtx); err != nil {
			logger.Warn(threadCtx, "Spynode client handler finished with error : %s", err)
		}
		logger.Info(threadCtx, "Spynode client handler finished")
		c.wait.Done()
	}()

	// Start ping thread
	c.wait.Add(1)
	go func() {
		logger.Info(threadCtx, "Spynode client pinging")
		if err := c.ping(threadCtx); err != nil {
			logger.Warn(threadCtx, "Pinger finished with error : %s", err)
			c.closeRequestedLock.Lock()
			c.closeRequested = true
			c.closeRequestedLock.Unlock()
		}
		logger.Info(threadCtx, "Spynode client finished pinging")
		c.wait.Done()
	}()

	return nil
}

func (c *RemoteClient) Close(ctx context.Context) {
	c.closeRequestedLock.Lock()
	c.closeRequested = true
	c.closeRequestedLock.Unlock()

	c.close(ctx)
	c.wait.Wait() // Wait for listen thread to finish

	// Clear close requested flag
	c.closeRequestedLock.Lock()
	c.closeRequested = false
	c.closeRequestedLock.Unlock()
}

func (c *RemoteClient) connect(ctx context.Context) (bool, error) {
	c.closeRequestedLock.Lock()
	c.closeRequested = false
	c.closeRequestedLock.Unlock()

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.conn != nil {
		return false, nil // already connected
	}

	start := time.Now()
	defer metrics.Elapsed(ctx, start, "SpyNodeClient.connect")

	var connectErr error
	for i := 0; i <= c.config.MaxRetries; i++ {
		if i > 0 {
			// Delay, then retry
			logger.Info(ctx, "Delaying %s before dial retry %d", c.config.RetryDelay,
				i)
			time.Sleep(c.config.RetryDelay.Duration)
		}

		// Check if we are trying to close
		c.closeRequestedLock.Lock()
		stop := c.closeRequested
		c.closeRequestedLock.Unlock()
		if stop {
			return false, connectErr
		}

		publicKey := c.config.ClientKey.PublicKey()
		clientID, err := bitcoin.NewHash20(bitcoin.Hash160(publicKey.Bytes()))
		if err != nil {
			return false, errors.Wrap(err, "client_id")
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("client_id", *clientID),
		}, "Connecting to spynode service")

		if err := c.generateSession(); err != nil {
			return false, errors.Wrap(err, "session")
		}

		var dialer net.Dialer
		conn, err := dialer.DialContext(ctx, "tcp", c.config.ServerAddress)
		if err != nil {
			logger.Warn(ctx, "Spynode service dial failed : %s", err)
			connectErr = err
			continue
		}

		// Create and sign register message
		register := &Register{
			Version:          RemoteClientVersion,
			Key:              publicKey,
			Hash:             c.hash,
			StartBlockHeight: c.config.StartBlockHeight,
			ConnectionType:   c.config.ConnectionType,
		}

		sigHash, err := register.SigHash()
		if err != nil {
			conn.Close()
			return false, errors.Wrap(err, "sig hash")
		}

		register.Signature, err = c.config.ClientKey.Sign(*sigHash)
		if err != nil {
			conn.Close()
			return false, errors.Wrap(err, "sign")
		}

		message := Message{Payload: register}
		if err := message.Serialize(conn); err != nil {
			conn.Close()
			return false, errors.Wrap(err, "send register")
		}

		c.conn = conn

		return true, nil
	}

	return false, connectErr
}

func (c *RemoteClient) close(ctx context.Context) {
	c.closeConnection(ctx)

	c.handlerChannelLock.Lock()
	if c.handlerChannelIsOpen {
		c.handlerChannelIsOpen = false
		close(c.handlerChannel)
	}
	c.handlerChannelLock.Unlock()
}

func (c *RemoteClient) closeConnection(ctx context.Context) {
	c.lock.Lock()
	if c.conn != nil {
		logger.Info(ctx, "Closing spynode connection")
		c.conn.Close()
		c.conn = nil
	}
	c.lock.Unlock()
}

func (c *RemoteClient) addHandlerMessage(ctx context.Context, msg MessagePayload) {
	c.handlerChannelLock.Lock()
	if c.handlerChannelIsOpen {
		c.handlerChannel <- msg
	}
	c.handlerChannelLock.Unlock()
}

// ping sends pings to keep the connection alive.
func (c *RemoteClient) ping(ctx context.Context) error {
	sinceLastPing := 0
	for {
		c.closeRequestedLock.Lock()
		stop := c.closeRequested
		c.closeRequestedLock.Unlock()

		if stop {
			return nil
		}

		c.lock.Lock()
		conn := c.conn
		c.lock.Unlock()

		if conn == nil {
			return nil // connection closed
		}

		sinceLastPing++
		if sinceLastPing >= 500 {
			timeStamp := uint64(time.Now().UnixNano())
			message := &Message{
				Payload: &Ping{TimeStamp: timeStamp},
			}
			if err := message.Serialize(conn); err != nil {
				return errors.Wrap(err, "send message")
			}
			sinceLastPing = 0
			logger.VerboseWithFields(ctx, []logger.Field{
				logger.Float64("timestamp", float64(timeStamp)/1000000000.0),
			}, "Sent ping")
		}

		time.Sleep(200 * time.Millisecond)
	}
}

// listen listens for incoming messages.
func (c *RemoteClient) listen(ctx context.Context) error {
	for {
		c.lock.Lock()
		conn := c.conn
		c.lock.Unlock()

		if conn == nil {
			logger.Info(ctx, "Connection closed")
			return nil // connection closed
		}

		m := &Message{}
		if err := m.Deserialize(conn); err != nil {
			var returnErr error
			if errors.Cause(err) == io.EOF || errors.Cause(err) == io.ErrUnexpectedEOF ||
				strings.Contains(err.Error(), "Closed") ||
				strings.Contains(err.Error(), "use of closed network connection") {
				logger.Info(ctx, "Server disconnected")
			} else {
				logger.Warn(ctx, "Failed to read incoming message : %s", err)
				returnErr = err
			}

			// Check if we are trying to close
			c.closeRequestedLock.Lock()
			stop := c.closeRequested
			c.closeRequestedLock.Unlock()
			if stop {
				c.close(ctx)
				return returnErr
			}

			c.closeConnection(ctx)
			if _, err := c.connect(ctx); err != nil {
				return errors.Wrap(err, "connect")
			}

			continue
		}

		var contextFields []logger.Field
		messageName, exists := MessageTypeNames[m.Payload.Type()]
		if !exists {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Uint64("remote_message", m.Payload.Type()),
			}, "Name not known for message type")
			contextFields = append(contextFields, logger.Uint64("remote_message", m.Payload.Type()))
		} else {
			contextFields = append(contextFields, logger.String("remote_message", messageName))
		}

		msgCtx := logger.ContextWithLogFields(ctx, contextFields...)

		// Handle message
		switch msg := m.Payload.(type) {
		case *AcceptRegister:
			logger.Info(msgCtx, "Received accept register")
			if !msg.Key.Equal(c.serverSessionKey) {
				logger.Error(msgCtx, "Wrong server session key returned : got %s, want %s", msg.Key,
					c.serverSessionKey)
				c.close(msgCtx)
				return ErrWrongKey
			}

			sigHash, err := msg.SigHash(c.hash)
			if err != nil {
				logger.Error(msgCtx, "Failed to create accept sig hash : %s", err)
				c.close(msgCtx)
				return errors.Wrap(err, "accept sig hash")
			}

			if !msg.Signature.Verify(*sigHash, msg.Key) {
				logger.Error(msgCtx, "Invalid server signature")
				c.close(msgCtx)
				return ErrBadSignature
			}

			logger.InfoWithFields(msgCtx, []logger.Field{
				logger.JSON("accept_register", msg),
			}, "Server accepted connection")
			c.lock.Lock()
			c.accepted = true
			c.lock.Unlock()

			c.addHandlerMessage(msgCtx, m.Payload)

		case *Tx:
			txid := *msg.Tx.TxHash()
			logger.InfoWithFields(msgCtx, []logger.Field{
				logger.Stringer("txid", txid),
				logger.Uint64("message_id", msg.ID),
			}, "Received tx")

			if msg.ID == 0 { // non-sequential message (from a request)
				c.requestLock.Lock()
				found := false
				for _, request := range c.getTxRequests {
					if request.response == nil && request.txid.Equal(&txid) {
						request.response = m
						found = true
						break
					}
				}
				c.requestLock.Unlock()

				if !found {
					logger.WarnWithFields(msgCtx, []logger.Field{
						logger.Stringer("txid", txid),
					}, "No matching request found for non-sequential tx")
				}
			} else if c.nextMessageID != msg.ID {
				logger.WarnWithFields(msgCtx, []logger.Field{
					logger.Uint64("expected_message_id", c.nextMessageID),
					logger.Uint64("message_id", msg.ID),
				}, "Wrong message ID in tx message")
			} else {
				c.nextMessageID = msg.ID + 1
				c.addHandlerMessage(msgCtx, m.Payload)
			}

		case *TxUpdate:
			logger.InfoWithFields(msgCtx, []logger.Field{
				logger.Stringer("txid", msg.TxID),
				logger.Uint64("message_id", msg.ID),
			}, "Received tx state")

			if c.nextMessageID != msg.ID {
				logger.WarnWithFields(msgCtx, []logger.Field{
					logger.Uint64("expected_message_id", c.nextMessageID),
					logger.Uint64("message_id", msg.ID),
				}, "Wrong message ID in tx update message")
			} else {
				c.nextMessageID = msg.ID + 1
				c.addHandlerMessage(msgCtx, m.Payload)
			}

		case *Headers:
			logger.InfoWithFields(msgCtx, []logger.Field{
				logger.Int("header_count", len(msg.Headers)),
				logger.Uint32("start_height", msg.StartHeight),
			}, "Received headers")

			requestFound := false
			c.requestLock.Lock()
			for _, request := range c.headersRequests {
				if request.response == nil && request.height == int(msg.RequestHeight) {
					request.response = m
					requestFound = true
					break
				}
			}
			c.requestLock.Unlock()

			if !requestFound {
				c.addHandlerMessage(msgCtx, m.Payload)
			}

		case *Header:
			blockHash := *msg.Header.BlockHash()
			logger.InfoWithFields(msgCtx, []logger.Field{
				logger.Stringer("block_hash", blockHash),
			}, "Received header")

			c.requestLock.Lock()
			for _, request := range c.headerRequests {
				if request.response == nil && request.hash.Equal(&blockHash) {
					request.response = m
					break
				}
			}
			c.requestLock.Unlock()

		case *FeeQuotes:
			logger.Info(msgCtx, "Received fee quotes")

			c.requestLock.Lock()
			for _, request := range c.feeQuoteRequests {
				if request.response == nil {
					request.response = m
				}
			}
			c.requestLock.Unlock()

			c.addHandlerMessage(msgCtx, m.Payload)

		case *InSync:
			logger.Info(msgCtx, "Received in sync")

			c.addHandlerMessage(msgCtx, m.Payload)

		case *ChainTip:
			logger.InfoWithFields(msgCtx, []logger.Field{
				logger.Stringer("hash", msg.Hash),
				logger.Uint32("height", msg.Height),
			}, "Received chain tip")

			c.addHandlerMessage(msgCtx, m.Payload)

		case *BaseTx:
			txid := *msg.Tx.TxHash()
			logger.InfoWithFields(msgCtx, []logger.Field{
				logger.Stringer("txid", txid),
			}, "Received base tx")

			c.requestLock.Lock()
			found := false
			for _, request := range c.getTxRequests {
				if request.response == nil && request.txid.Equal(&txid) {
					request.response = m
					found = true
					break
				}
			}
			c.requestLock.Unlock()

			if !found {
				logger.WarnWithFields(msgCtx, []logger.Field{
					logger.Stringer("txid", txid),
				}, "No matching request found for base tx")
			}

		case *Accept:
			logger.Info(msgCtx, "Received accept")
			if msg.Hash != nil {
				switch msg.MessageType {
				case MessageTypeSendTx:
					logger.InfoWithFields(msgCtx, []logger.Field{
						logger.Stringer("txid", msg.Hash),
					}, "Received accept for send tx")

					found := false
					c.requestLock.Lock()
					for _, request := range c.sendTxRequests {
						request.lock.Lock()
						if request.response == nil && request.txid.Equal(msg.Hash) {
							request.response = m
							request.lock.Unlock()
							found = true
							break
						}
						request.lock.Unlock()
					}
					c.requestLock.Unlock()

					if !found {
						logger.WarnWithFields(msgCtx, []logger.Field{
							logger.Stringer("txid", msg.Hash),
						}, "No matching request found for send tx accept")
					}

				case MessageTypeReprocessTx:
					logger.InfoWithFields(msgCtx, []logger.Field{
						logger.Stringer("txid", msg.Hash),
					}, "Received accept for reprocess tx")

					found := false
					c.requestLock.Lock()
					for _, request := range c.reprocessTxRequests {
						request.lock.Lock()
						if request.response == nil && request.txid.Equal(msg.Hash) {
							request.response = m
							request.lock.Unlock()
							found = true
							break
						}
						request.lock.Unlock()
					}
					c.requestLock.Unlock()

					if !found {
						logger.WarnWithFields(msgCtx, []logger.Field{
							logger.Stringer("txid", msg.Hash),
						}, "No matching request found for mark header invalid accept")
					}

				case MessageTypeMarkHeaderInvalid:
					logger.InfoWithFields(msgCtx, []logger.Field{
						logger.Stringer("block_hash", msg.Hash),
					}, "Received accept for mark header invalid")

					found := false
					c.requestLock.Lock()
					for _, request := range c.markHeaderInvalidRequests {
						request.lock.Lock()
						if request.response == nil && request.blockHash.Equal(msg.Hash) {
							request.response = m
							request.lock.Unlock()
							found = true
							break
						}
						request.lock.Unlock()
					}
					c.requestLock.Unlock()

					if !found {
						logger.WarnWithFields(msgCtx, []logger.Field{
							logger.Stringer("block_hash", msg.Hash),
						}, "No matching request found for mark header invalid accept")
					}

				case MessageTypeMarkHeaderNotInvalid:
					logger.InfoWithFields(msgCtx, []logger.Field{
						logger.Stringer("block_hash", msg.Hash),
					}, "Received accept for mark header not invalid")

					found := false
					c.requestLock.Lock()
					for _, request := range c.markHeaderNotInvalidRequests {
						request.lock.Lock()
						if request.response == nil && request.blockHash.Equal(msg.Hash) {
							request.response = m
							request.lock.Unlock()
							found = true
							break
						}
						request.lock.Unlock()
					}
					c.requestLock.Unlock()

					if !found {
						logger.WarnWithFields(msgCtx, []logger.Field{
							logger.Stringer("block_hash", msg.Hash),
						}, "No matching request found for mark header not invalid accept")
					}

				default:
					logger.InfoWithFields(msgCtx, []logger.Field{
						logger.Uint64("type", msg.MessageType),
						logger.String("name", NameForMessageType(msg.MessageType)),
						logger.Stringer("hash", msg.Hash),
					}, "Received accept for unsupported message")

				}
			} else {
				logger.Info(msgCtx, "Received accept with no hash")
			}

		case *Reject:
			c.lock.Lock()
			accepted := c.accepted
			c.lock.Unlock()

			logger.Info(msgCtx, "Received reject")

			if !accepted {
				// Service rejected registration
				logger.Info(msgCtx, "Reject registration")
				c.close(msgCtx)
				return NewRejectError(msg.Code, msg.Message)
			}

			if msg.Hash != nil {
				found := false

				switch msg.MessageType {
				case MessageTypeSendTx:
					logger.WarnWithFields(msgCtx, []logger.Field{
						logger.Stringer("txid", msg.Hash),
					}, "Received reject for send tx : %s", msg.Message)

					c.requestLock.Lock()
					for _, request := range c.sendTxRequests {
						request.lock.Lock()
						if request.response == nil && request.txid.Equal(msg.Hash) {
							request.response = m
							request.lock.Unlock()
							found = true
							break
						}
						request.lock.Unlock()
					}
					c.requestLock.Unlock()

					if !found {
						logger.WarnWithFields(msgCtx, []logger.Field{
							logger.Stringer("txid", msg.Hash),
						}, "No matching request found for send tx reject")
					}

				case MessageTypeGetTx:
					logger.WarnWithFields(msgCtx, []logger.Field{
						logger.Stringer("txid", msg.Hash),
					}, "Received reject for get tx : %s", msg.Message)

					c.requestLock.Lock()
					for _, request := range c.getTxRequests {
						request.lock.Lock()
						if request.response == nil && request.txid.Equal(msg.Hash) {
							request.response = m
							request.lock.Unlock()
							found = true
							break
						}
						request.lock.Unlock()
					}
					c.requestLock.Unlock()

					if !found {
						logger.WarnWithFields(msgCtx, []logger.Field{
							logger.Stringer("txid", msg.Hash),
						}, "No matching request found for get tx reject")
					}

				case MessageTypeGetHeader:
					logger.WarnWithFields(msgCtx, []logger.Field{
						logger.Stringer("block_hash", msg.Hash),
					}, "Received reject for get header : %s", msg.Message)

					c.requestLock.Lock()
					for _, request := range c.headerRequests {
						request.lock.Lock()
						if request.response == nil && request.hash.Equal(msg.Hash) {
							request.response = m
							request.lock.Unlock()
							found = true
							break
						}
						request.lock.Unlock()
					}
					c.requestLock.Unlock()

					if !found {
						logger.WarnWithFields(msgCtx, []logger.Field{
							logger.Stringer("txid", msg.Hash),
						}, "No matching request found for get header reject")
					}

				case MessageTypeGetFeeQuotes:
					logger.Warn(msgCtx, "Received reject for get fee quotes : %s", msg.Message)

					c.requestLock.Lock()
					for _, request := range c.feeQuoteRequests {
						request.lock.Lock()
						if request.response == nil {
							request.response = m
							request.lock.Unlock()
						}
						request.lock.Unlock()
					}
					c.requestLock.Unlock()

				case MessageTypeReprocessTx:
					logger.WarnWithFields(msgCtx, []logger.Field{
						logger.Stringer("txid", msg.Hash),
					}, "Received reject for reprocess tx : %s", msg.Message)

					c.requestLock.Lock()
					for _, request := range c.reprocessTxRequests {
						request.lock.Lock()
						if request.response == nil && request.txid.Equal(msg.Hash) {
							request.response = m
							request.lock.Unlock()
							found = true
							break
						}
						request.lock.Unlock()
					}
					c.requestLock.Unlock()

					if !found {
						logger.WarnWithFields(msgCtx, []logger.Field{
							logger.Stringer("txid", msg.Hash),
						}, "No matching request found for reprocess tx reject")
					}

				case MessageTypeMarkHeaderInvalid:
					logger.InfoWithFields(msgCtx, []logger.Field{
						logger.Stringer("block_hash", msg.Hash),
					}, "Received reject for mark header invalid")

					found := false
					c.requestLock.Lock()
					for _, request := range c.markHeaderInvalidRequests {
						request.lock.Lock()
						if request.response == nil && request.blockHash.Equal(msg.Hash) {
							request.response = m
							request.lock.Unlock()
							found = true
							break
						}
						request.lock.Unlock()
					}
					c.requestLock.Unlock()

					if !found {
						logger.WarnWithFields(msgCtx, []logger.Field{
							logger.Stringer("block_hash", msg.Hash),
						}, "No matching request found for mark header invalid reject")
					}

				case MessageTypeMarkHeaderNotInvalid:
					logger.InfoWithFields(msgCtx, []logger.Field{
						logger.Stringer("block_hash", msg.Hash),
					}, "Received reject for mark header not invalid")

					found := false
					c.requestLock.Lock()
					for _, request := range c.markHeaderNotInvalidRequests {
						request.lock.Lock()
						if request.response == nil && request.blockHash.Equal(msg.Hash) {
							request.response = m
							request.lock.Unlock()
							found = true
							break
						}
						request.lock.Unlock()
					}
					c.requestLock.Unlock()

					if !found {
						logger.WarnWithFields(msgCtx, []logger.Field{
							logger.Stringer("block_hash", msg.Hash),
						}, "No matching request found for mark header not invalid reject")
					}

				default:
					logger.InfoWithFields(msgCtx, []logger.Field{
						logger.Uint64("type", msg.MessageType),
						logger.String("name", NameForMessageType(msg.MessageType)),
						logger.Stringer("hash", msg.Hash),
					}, "Received reject for unsupported message")
				}
			} else {
				logger.Info(msgCtx, "Received reject with no hash")
			}

		case *Ping:
			logger.VerboseWithFields(msgCtx, []logger.Field{
				logger.Float64("timestamp", float64(msg.TimeStamp)/1000000000.0),
			}, "Received ping")

		case *Pong:
			logger.VerboseWithFields(msgCtx, []logger.Field{
				logger.Float64("request_timestamp", float64(msg.RequestTimeStamp)/1000000000.0),
				logger.Float64("timestamp", float64(msg.TimeStamp)/1000000000.0),
				logger.Float64("delta", float64(msg.TimeStamp-msg.RequestTimeStamp)/1000000000.0),
			}, "Received pong")

		default:
			logger.Error(msgCtx, "Unknown message type : %d", msg.Type())

		}
	}
}

func (c *RemoteClient) handle(ctx context.Context) error {
	for msg := range c.handlerChannel {
		switch m := msg.(type) {
		case *AcceptRegister, *ChainTip:
			c.handlerLock.Lock()
			for _, handler := range c.handlers {
				handler.HandleMessage(ctx, msg)
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
				handler.HandleHeaders(ctx, m)
			}
			c.handlerLock.Unlock()

		case *TxUpdate:
			c.handlerLock.Lock()
			for _, handler := range c.handlers {
				handler.HandleTxUpdate(ctx, m)
			}
			c.handlerLock.Unlock()

		case *Tx:
			c.handlerLock.Lock()
			for _, handler := range c.handlers {
				handler.HandleTx(ctx, m)
			}
			c.handlerLock.Unlock()
		}
	}

	return nil
}

// generateSession generates session keys from root keys.
func (c *RemoteClient) generateSession() error {
	for { // loop through any out of range keys
		var err error

		// Generate random hash
		c.hash, err = bitcoin.GenerateSeedValue()
		if err != nil {
			return errors.Wrap(err, "generate hash")
		}

		// Derive session keys
		c.serverSessionKey, err = bitcoin.NextPublicKey(c.config.ServerKey, c.hash)
		if err != nil {
			if errors.Cause(err) == bitcoin.ErrOutOfRangeKey {
				continue // try with a new hash
			}
			return errors.Wrap(err, "next public key")
		}

		c.sessionKey, err = bitcoin.NextKey(c.config.ClientKey, c.hash)
		if err != nil {
			if errors.Cause(err) == bitcoin.ErrOutOfRangeKey {
				continue // try with a new hash
			}
			return errors.Wrap(err, "next key")
		}

		return nil
	}
}
