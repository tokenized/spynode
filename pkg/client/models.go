package client

import (
	"fmt"
	"io"
	"math"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/merchant_api"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
)

const (
	// UnconfirmedBlockHeight is the block height specified when the transaction is unconfirmed.
	UnconfirmedBlockHeight = math.MaxUint32

	// MessageTypeRegister is the type of a register message.
	MessageTypeRegister = uint64(1)

	// MessageTypeSubscribePushData is the type of a subscribe push data message.
	MessageTypeSubscribePushData = uint64(11)

	// MessageTypeUnsubscribePushData is the type of an unsubscribe push data message.
	MessageTypeUnsubscribePushData = uint64(12)

	// MessageTypeSubscribeTx is the type of a subscribe tx message.
	MessageTypeSubscribeTx = uint64(13)

	// MessageTypeUnsubscribeTx is the type of an unsubscribe tx message.
	MessageTypeUnsubscribeTx = uint64(14)

	// MessageTypeSubscribeOutputs is the type of a subscribe tx message.
	MessageTypeSubscribeOutputs = uint64(15)

	// MessageTypeUnsubscribeOutputs is the type of an unsubscribe tx message.
	MessageTypeUnsubscribeOutputs = uint64(16)

	// MessageTypeSubscribeHeaders is the type of a subscribe headers message.
	MessageTypeSubscribeHeaders = uint64(17)

	// MessageTypeUnsubscribeHeaders is the type of an unsubscribe headers message.
	MessageTypeUnsubscribeHeaders = uint64(18)

	// MessageTypeSubscribeContracts is the type of a subscribe contracts message.
	MessageTypeSubscribeContracts = uint64(19)

	// MessageTypeUnsubscribeContracts is the type of an unsubscribe contracts message.
	MessageTypeUnsubscribeContracts = uint64(20)

	// MessageTypeReady is the type of a ready message.
	MessageTypeReady = uint64(30)

	// MessageTypeGetChainTip requests chain tip info.
	MessageTypeGetChainTip = uint64(41)

	// MessageTypeGetHeaders requests headers.
	MessageTypeGetHeaders = uint64(42)

	// MessageTypeSendTx sends a tx to the Bitcoin network.
	MessageTypeSendTx = uint64(43)

	// MessageTypeGetTx requests a transaction.
	MessageTypeGetTx = uint64(44)

	// MessageTypeGetHeader requests a header by its hash.
	MessageTypeGetHeader = uint64(45)

	// MessageTypeGetFeeQuote requests a fee quote.
	MessageTypeGetFeeQuotes = uint64(46)

	// MessageTypePostMerkleProofs posts a merkle proof.
	MessageTypePostMerkleProofs = uint64(47)

	// MessageTypeReprocessTx requests that the tx be processed if it wasn't already.
	MessageTypeReprocessTx = uint64(51)

	// MessageTypeMarkHeaderInvalid requests that a header hash be marked invalid. This will cause
	// the chain containing that header to be invalid and the next longest chain is activated.
	MessageTypeMarkHeaderInvalid = uint64(52)

	// MessageTypeMarkHeaderNotInvalid requests that a header hash that was previously marked
	// invalid be valid again.
	MessageTypeMarkHeaderNotInvalid = uint64(53)

	// MessageTypeAcceptRegister is the type of an accept register message.
	MessageTypeAcceptRegister = uint64(101)

	// MessageTypeBaseTx is the type of a base tx message.
	MessageTypeBaseTx = uint64(110)

	// MessageTypeTx is the type of a tx message.
	MessageTypeTx = uint64(111)

	// MessageTypeTxUpdate is the type of a tx update message.
	MessageTypeTxUpdate = uint64(112)

	// MessageTypeInSync is in sync info.
	MessageTypeInSync = uint64(121)

	// MessageTypeChainTip is chain tip info.
	MessageTypeChainTip = uint64(122)

	// MessageTypeHeaders is headers.
	MessageTypeHeaders = uint64(123)

	// MessageTypeHeader is a header.
	MessageTypeHeader = uint64(124)

	// MessageTypeFeeQuotes is headers.
	MessageTypeFeeQuotes = uint64(125)

	// MessageTypeAccept is an accept of the previous request.
	MessageTypeAccept = uint64(200)

	// MessageTypeReject is a rejection of the previous request.
	MessageTypeReject = uint64(201)

	// MessageTypePing is a ping message to keep the connection alive.
	MessageTypePing = uint64(301)
	MessageTypePong = uint64(302)

	// ConnectionTypeFull is the normal connection type the allows control and receiving data
	// messages.
	ConnectionTypeFull = ConnectionType(1)

	// ConnectionTypeControl is a control only connection type that does not receive data messages.
	ConnectionTypeControl = ConnectionType(2)

	RejectCodeUnspecified = RejectCode(0)
	RejectCodeTimeout     = RejectCode(1)
	RejectCodeInvalid     = RejectCode(2)
	RejectCodeNotFound    = RejectCode(3)
)

type ConnectionType uint8
type RejectCode uint32

var (
	MessageTypeNames = map[uint64]string{
		MessageTypeRegister:             "register",
		MessageTypeSubscribePushData:    "subscribe_push_data",
		MessageTypeUnsubscribePushData:  "unsubscribe_push_data",
		MessageTypeSubscribeTx:          "subscribe_tx",
		MessageTypeUnsubscribeTx:        "unsubscribe_tx",
		MessageTypeSubscribeOutputs:     "subscribe_outputs",
		MessageTypeUnsubscribeOutputs:   "unsubscribe_outputs",
		MessageTypeSubscribeHeaders:     "subscribe_headers",
		MessageTypeUnsubscribeHeaders:   "unsubscribe_headers",
		MessageTypeSubscribeContracts:   "subscribe_contracts",
		MessageTypeUnsubscribeContracts: "unsubscribe_contracts",
		MessageTypeReady:                "ready",
		MessageTypeGetChainTip:          "get_chain_tip",
		MessageTypeGetHeaders:           "get_headers",
		MessageTypeGetHeader:            "get_header",
		MessageTypeGetFeeQuotes:         "get_fee_quotes",
		MessageTypeSendTx:               "send_tx",
		MessageTypeGetTx:                "get_tx",
		MessageTypeReprocessTx:          "reprocess_tx",
		MessageTypeMarkHeaderInvalid:    "mark_header_invalid",
		MessageTypeMarkHeaderNotInvalid: "mark_header_not_invalid",
		MessageTypeAcceptRegister:       "accept_register",
		MessageTypeBaseTx:               "base_tx",
		MessageTypeTx:                   "tx",
		MessageTypeTxUpdate:             "tx_update",
		MessageTypeInSync:               "in_sync",
		MessageTypeChainTip:             "chain_tip",
		MessageTypeHeaders:              "headers",
		MessageTypeHeader:               "header",
		MessageTypeFeeQuotes:            "fee_quotes",
		MessageTypePostMerkleProofs:     "post_merkle_proofs",
		MessageTypeAccept:               "accept",
		MessageTypeReject:               "reject",
		MessageTypePing:                 "ping",
		MessageTypePong:                 "pong",
	}
)

type Message struct {
	Payload MessagePayload
}

type MessagePayload interface {
	// Deserialize reads the message from a reader.
	Deserialize(io.Reader) error

	// Serialize writes the message to a writer.
	Serialize(io.Writer) error

	// Type returns they type of the message.
	Type() uint64
}

func (v *ConnectionType) UnmarshalJSON(data []byte) error {
	if len(data) < 2 {
		return fmt.Errorf("Too short for ConnectionType : %d", len(data))
	}

	value := string(data[1 : len(data)-1])
	switch value {
	case "full", "1":
		*v = ConnectionTypeFull
	case "control", "2":
		*v = ConnectionTypeControl

	default:
		return fmt.Errorf("Unknown connection type value \"%s\"", value)
	}

	return nil
}

func (v ConnectionType) MarshalJSON() ([]byte, error) {
	s := v.String()
	if len(s) == 0 {
		return []byte("null"), nil
	}

	return []byte(fmt.Sprintf("\"%s\"", s)), nil
}

func (v ConnectionType) MarshalText() ([]byte, error) {
	switch v {
	case ConnectionTypeFull:
		return []byte("full"), nil
	case ConnectionTypeControl:
		return []byte("control"), nil
	}

	return nil, fmt.Errorf("Unknown connection type value \"%d\"", uint8(v))
}

func (v *ConnectionType) UnmarshalText(text []byte) error {
	switch string(text) {
	case "full", "1":
		*v = ConnectionTypeFull
	case "control", "2":
		*v = ConnectionTypeControl

	default:
		return fmt.Errorf("Unknown connection type value \"%s\"", string(text))
	}

	return nil
}

func (v ConnectionType) String() string {
	switch v {
	case ConnectionTypeFull:
		return "full"
	case ConnectionTypeControl:
		return "control"
	}

	return ""
}

func (v *RejectCode) UnmarshalJSON(data []byte) error {
	if len(data) < 2 {
		return fmt.Errorf("Too short for RejectCode : %d", len(data))
	}

	value := string(data[1 : len(data)-1])
	switch value {
	case "unspecified", "":
		*v = RejectCodeUnspecified
	case "timeout":
		*v = RejectCodeTimeout
	case "invalid":
		*v = RejectCodeInvalid
	case "not_found":
		*v = RejectCodeNotFound

	default:
		return fmt.Errorf("Unknown reject code value \"%s\"", value)
	}

	return nil
}

func (v RejectCode) MarshalJSON() ([]byte, error) {
	s := v.String()
	if len(s) == 0 {
		return []byte("null"), nil
	}

	return []byte(fmt.Sprintf("\"%s\"", s)), nil
}

func (v RejectCode) MarshalText() ([]byte, error) {
	switch v {
	case RejectCodeUnspecified:
		return []byte("unspecified"), nil
	case RejectCodeTimeout:
		return []byte("timeout"), nil
	case RejectCodeInvalid:
		return []byte("invalid"), nil
	case RejectCodeNotFound:
		return []byte("not_found"), nil
	}

	return nil, fmt.Errorf("Unknown reject code value \"%d\"", uint8(v))
}

func (v *RejectCode) UnmarshalText(text []byte) error {
	switch string(text) {
	case "unspecified", "":
		*v = RejectCodeUnspecified
	case "timeout":
		*v = RejectCodeTimeout
	case "invalid":
		*v = RejectCodeInvalid
	case "not_found":
		*v = RejectCodeNotFound

	default:
		return fmt.Errorf("Unknown reject code value \"%s\"", string(text))
	}

	return nil
}

func (v RejectCode) String() string {
	switch v {
	case RejectCodeUnspecified:
		return "unspecified"
	case RejectCodeTimeout:
		return "timeout"
	case RejectCodeInvalid:
		return "invalid"
	case RejectCodeNotFound:
		return "not_found"
	}

	return "unknown"
}

// Client to Server Messages -----------------------------------------------------------------------

// Register is the first message received from the client. It can be from a previous connection or
// it can be a new client based on whether the Key has been seen before.
type Register struct {
	Version          uint8             // Version of communication protocol
	Key              bitcoin.PublicKey // Client's public key
	Hash             bitcoin.Hash32    // For deriving ephemeral keys for use during this connection.
	StartBlockHeight uint32            // For new clients this is the starting height for data.
	ChainTip         bitcoin.Hash32    // The client's current chain tip block hash.
	ConnectionType   ConnectionType    // The type of the connection.
	Signature        bitcoin.Signature // Signature of this messaage to prove key ownership.
}

// SubscribePushData adds new push data hashes used to filter relevant transactions. These and UTXOs for
// relevant transaction outputs are automatically retained between sessions.
type SubscribePushData struct {
	PushDatas [][]byte
}

// UnsubscribePushData removes push data hashes used to filter relevant transactions.
type UnsubscribePushData struct {
	PushDatas [][]byte
}

// SubscribeTx adds a new txid used to filter relevant transactions. Indexes are the indexes of the
// outputs that need to be monitored for spending.
type SubscribeTx struct {
	TxID    bitcoin.Hash32
	Indexes []uint32
}

// UnsubscribeTx removes the txid used to filter relevant transactions.
type UnsubscribeTx struct {
	TxID    bitcoin.Hash32
	Indexes []uint32
}

// SubscribeOutputs adds outputs that need to be monitored for spending.
type SubscribeOutputs struct {
	Outputs []*wire.OutPoint
}

// UnsubscribeOutputs removes the outputs used to filter relevant transactions.
type UnsubscribeOutputs struct {
	Outputs []*wire.OutPoint
}

// SubscribeHeaders requests that all new headers be sent as they are known.
type SubscribeHeaders struct{}

// UnsubscribeHeaders requests that headers no longer be sent automatically. They can still be
// directly requested.
type UnsubscribeHeaders struct{}

// SubscribeContracts requests that all contract-wide transactions be sent.
// ContractFormations/InstrumentCreations
type SubscribeContracts struct{}

// UnsubscribeContracts requests that all contract-wide transactions no longer be sent.
// ContractFormations/InstrumentCreations
type UnsubscribeContracts struct{}

// Ready tells the server that it can start syncing the client. This is sent after all initial
// Subscribe/Unsubscribe messages.
type Ready struct {
	NextMessageID uint64
}

// GetChainTip requests the height and hash of the lastest block.
type GetChainTip struct{}

// GetHeaders requests a set of headers.
type GetHeaders struct {
	RequestHeight int32  // -1 for most recent
	MaxCount      uint32 // max number of headers to return
}

// SendTx requests that tx be broadcast to the Bitcoin network. Indexes are the indexes of the
// outputs that need to be monitored for spending.
type SendTx struct {
	Tx      *wire.MsgTx
	Indexes []uint32
}

// GetTx requests a tx by its hash.
type GetTx struct {
	TxID bitcoin.Hash32
}

type GetHeader struct {
	BlockHash bitcoin.Hash32
}

type GetFeeQuotes struct {
}

// PostMerkleProofs requests a tx by its hash.
type PostMerkleProofs struct {
	MerkleProofs []*merkle_proof.MerkleProof
}

// ReprocessTx requests a tx be fetched and processed if it wasn't already.
type ReprocessTx struct {
	TxID      bitcoin.Hash32
	ClientIDs []bitcoin.Hash20 // clients to send tx to for processing
}

// MarkHeaderInvalid requests that a header hash be marked invalid. This will cause the chain
// containing that header to be invalid and the next longest chain is activated.
type MarkHeaderInvalid struct {
	BlockHash bitcoin.Hash32
}

// MarkHeaderNotInvalid requests that a header hash that was previously marked invalid be valid again.
type MarkHeaderNotInvalid struct {
	BlockHash bitcoin.Hash32
}

// Server to Client Messages -----------------------------------------------------------------------

type AcceptRegister struct {
	Key           bitcoin.PublicKey // Server's public key
	PushDataCount uint64
	UTXOCount     uint64
	MessageCount  uint64
	Signature     bitcoin.Signature // Signature of public key and the hash to prove key ownership.
}

// BaseTx is a just the basic transaction.
type BaseTx struct {
	Tx *wire.MsgTx
}

// Tx is a new transaction that is relevant to the client.
type Tx struct {
	ID      uint64 // message id to uniquely identify this message and the order of messages.
	Tx      *wire.MsgTx
	Outputs []*wire.TxOut // outputs being spent by inputs in Tx
	State   TxState       // initial state
}

func (tx Tx) TxID() bitcoin.Hash32 {
	return *tx.Tx.TxHash()
}

func (tx Tx) InputCount() int {
	return len(tx.Tx.TxIn)
}

func (tx Tx) Input(index int) *wire.TxIn {
	return tx.Tx.TxIn[index]
}

func (tx Tx) InputLockingScript(index int) (bitcoin.Script, error) {
	if index >= len(tx.Outputs) {
		return nil, errors.New("Index out of range")
	}

	return tx.Outputs[index].LockingScript, nil
}

func (tx Tx) OutputCount() int {
	return len(tx.Tx.TxOut)
}

func (tx Tx) Output(index int) *wire.TxOut {
	return tx.Tx.TxOut[index]
}

func (tx Tx) InputValue(index int) (uint64, error) {
	if index >= len(tx.Outputs) {
		return 0, errors.New("Index out of range")
	}

	return tx.Outputs[index].Value, nil
}

// TxUpdate is an updated state for a transaction.
type TxUpdate struct {
	ID    uint64 // message id to uniquely identify this message and the order of messages.
	TxID  bitcoin.Hash32
	State TxState
}

// Headers is a list of block headers.
type Headers struct {
	RequestHeight int32  // height of request. zero if not a response to a request.
	StartHeight   uint32 // height of the first header, other headers are consecutive.
	Headers       []*wire.BlockHeader
}

type Header struct {
	Header      wire.BlockHeader
	BlockHeight uint32
	IsMostPOW   bool
}

type FeeQuotes struct {
	FeeQuotes merchant_api.FeeQuotes
}

// InSync is a notification that the messages are "up to date" with the network.
type InSync struct{}

// ChainTip is information about the latest block in the most POW chain.
type ChainTip struct {
	Height uint32
	Hash   bitcoin.Hash32
}

// Accept is an accept of the previous request
type Accept struct {
	MessageType uint64          // type of the message being rejected
	Hash        *bitcoin.Hash32 // optional identifier for the rejected item (tx)
}

// Reject is a rejection of the previous request
type Reject struct {
	MessageType uint64          // type of the message being rejected
	Hash        *bitcoin.Hash32 // optional identifier for the rejected item (tx)
	Code        RejectCode      // code representing the reason for the reject
	Message     string
}

// Ping is a ping to keep the connection live.
type Ping struct {
	TimeStamp uint64 // Current time
}

// Pong is a ping to keep the connection live.
type Pong struct {
	RequestTimeStamp uint64
	TimeStamp        uint64 // Current time
}

// Sub structures ----------------------------------------------------------------------------------

// TxState is state of a transaction.
type TxState struct {
	Safe             bool         // initial acceptance after checking for double spends
	UnSafe           bool         // transaction has known double spends or other unsafe attributes
	Cancelled        bool         // transaction has had a conflicting transaction confirmed
	UnconfirmedDepth uint32       // mempool chain depth
	MerkleProof      *MerkleProof // proof the txid is in the block.
}

// MerkleProof is the proof a txid is in the tree referenced by the merkle root of a block header.
type MerkleProof struct {
	Index             uint64 // Index of tx in block
	Path              []bitcoin.Hash32
	BlockHeader       wire.BlockHeader
	DuplicatedIndexes []uint64
}
