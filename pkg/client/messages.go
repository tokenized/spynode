package client

import (
	"crypto/sha256"
	"encoding/binary"
	"io"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/merchant_api"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
)

// Deserialize reads the message from a reader.
func (m *Message) Deserialize(r io.Reader) error {
	t, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "type")
	}

	m.Payload = PayloadForType(t)
	if m.Payload == nil {
		return errors.Wrapf(ErrUnknownMessageType, "%d", t)
	}

	if err := m.Payload.Deserialize(r); err != nil {
		return errors.Wrap(err, "payload")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m Message) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.Payload.Type()); err != nil {
		return errors.Wrap(err, "type")
	}

	if err := m.Payload.Serialize(w); err != nil {
		return errors.Wrap(err, "payload")
	}

	return nil
}

// SerializeWithKey serializes the payload while calculating a hash, then signs with the key and
// serializes the signature.
func (m Message) SerializeWithKey(w io.Writer, key bitcoin.Key) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.Payload.Type()); err != nil {
		return errors.Wrap(err, "type")
	}

	// Write into SHA256 so we can calculate a hash of the payload.
	// s := sha256.New()
	// ws := io.MultiWriter(w, s)
	if err := m.Payload.Serialize(w); err != nil {
		return errors.Wrap(err, "payload")
	}

	// var err error
	// sh := sha256.Sum256(s.Sum(nil))
	// m.Signature, err = key.Sign(sh[:])
	// if err != nil {
	// 	return errors.Wrap(err, "sign")
	// }
	// if err := m.Signature.Serialize(w); err != nil {
	// 	return errors.Wrap(err, "signature")
	// }

	return nil
}

func NameForMessageType(t uint64) string {
	messageName, exists := MessageTypeNames[t]
	if !exists {
		return ""
	}
	return messageName
}

func (m Message) Name() string {
	return NameForMessageType(m.Payload.Type())
}

func IsHandshakeType(t uint64) bool {
	switch t {
	case MessageTypeRegister, MessageTypeReady,
		MessageTypeSubscribePushData, MessageTypeUnsubscribePushData,
		MessageTypeSubscribeTx, MessageTypeUnsubscribeTx,
		MessageTypeSubscribeOutputs, MessageTypeUnsubscribeOutputs,
		MessageTypeSubscribeHeaders, MessageTypeUnsubscribeHeaders,
		MessageTypeSubscribeContracts, MessageTypeUnsubscribeContracts:
		return true
	default:
		return false
	}
}

func (m Message) IsHandshakeType() bool {
	return IsHandshakeType(m.Payload.Type())
}

func IsRequestType(t uint64) bool {
	switch t {
	case MessageTypeGetChainTip, MessageTypeGetHeaders, MessageTypeSendTx,
		MessageTypeSendExpandedTx, MessageTypeSaveTxs, MessageTypeGetTx, MessageTypeGetHeader,
		MessageTypeGetFeeQuotes, MessageTypePostMerkleProofs, MessageTypeReprocessTx,
		MessageTypeMarkHeaderInvalid, MessageTypeMarkHeaderNotInvalid, MessageTypePing:
		return true
	default:
		return false
	}
}

func (m Message) IsRequestType() bool {
	return IsRequestType(m.Payload.Type())
}

func IsResponseType(t uint64) bool {
	switch t {
	case MessageTypeAcceptRegister, MessageTypeBaseTx, MessageTypeTx, MessageTypeTxUpdate,
		MessageTypeInSync, MessageTypeChainTip, MessageTypeHeaders, MessageTypeHeader,
		MessageTypeFeeQuotes, MessageTypeAccept, MessageTypeReject, MessageTypePong:
		return true
	default:
		return false
	}
}

func (m Message) IsResponseType() bool {
	return IsResponseType(m.Payload.Type())
}

// PayloadForType returns the struct for the specified type.
func PayloadForType(t uint64) MessagePayload {
	switch t {
	case MessageTypeRegister:
		return &Register{}
	case MessageTypeSubscribePushData:
		return &SubscribePushData{}
	case MessageTypeUnsubscribePushData:
		return &UnsubscribePushData{}
	case MessageTypeSubscribeTx:
		return &SubscribeTx{}
	case MessageTypeUnsubscribeTx:
		return &UnsubscribeTx{}
	case MessageTypeSubscribeOutputs:
		return &SubscribeOutputs{}
	case MessageTypeUnsubscribeOutputs:
		return &UnsubscribeOutputs{}
	case MessageTypeSubscribeHeaders:
		return &SubscribeHeaders{}
	case MessageTypeUnsubscribeHeaders:
		return &UnsubscribeHeaders{}
	case MessageTypeSubscribeContracts:
		return &SubscribeContracts{}
	case MessageTypeUnsubscribeContracts:
		return &UnsubscribeContracts{}
	case MessageTypeReady:
		return &Ready{}
	case MessageTypeGetChainTip:
		return &GetChainTip{}
	case MessageTypeGetHeaders:
		return &GetHeaders{}
	case MessageTypeSendTx:
		return &SendTx{}
	case MessageTypeSendExpandedTx:
		return &SendExpandedTx{}
	case MessageTypeSaveTxs:
		return &SaveTxs{}
	case MessageTypeGetTx:
		return &GetTx{}
	case MessageTypeGetHeader:
		return &GetHeader{}
	case MessageTypeGetFeeQuotes:
		return &GetFeeQuotes{}
	case MessageTypePostMerkleProofs:
		return &PostMerkleProofs{}
	case MessageTypeReprocessTx:
		return &ReprocessTx{}
	case MessageTypeMarkHeaderInvalid:
		return &MarkHeaderInvalid{}
	case MessageTypeMarkHeaderNotInvalid:
		return &MarkHeaderNotInvalid{}

	case MessageTypeAcceptRegister:
		return &AcceptRegister{}
	case MessageTypeBaseTx:
		return &BaseTx{}
	case MessageTypeTx:
		return &Tx{}
	case MessageTypeTxUpdate:
		return &TxUpdate{}
	case MessageTypeInSync:
		return &InSync{}
	case MessageTypeChainTip:
		return &ChainTip{}
	case MessageTypeHeaders:
		return &Headers{}
	case MessageTypeHeader:
		return &Header{}
	case MessageTypeFeeQuotes:
		return &FeeQuotes{}

	case MessageTypeAccept:
		return &Accept{}
	case MessageTypeReject:
		return &Reject{}

	case MessageTypePing:
		return &Ping{}
	case MessageTypePong:
		return &Pong{}
	}

	return nil
}

// Client to Server Messages -----------------------------------------------------------------------

// Deserialize reads the message from a reader.
func (m *Register) Deserialize(r io.Reader) error {
	if err := binary.Read(r, Endian, &m.Version); err != nil {
		return errors.Wrap(err, "version")
	}

	if err := m.Key.Deserialize(r); err != nil {
		return errors.Wrap(err, "key")
	}

	if err := m.Hash.Deserialize(r); err != nil {
		return errors.Wrap(err, "hash")
	}

	if err := binary.Read(r, Endian, &m.StartBlockHeight); err != nil {
		return errors.Wrap(err, "start block height")
	}

	if err := m.ChainTip.Deserialize(r); err != nil {
		return errors.Wrap(err, "chain tip")
	}

	if err := binary.Read(r, Endian, &m.ConnectionType); err != nil {
		return errors.Wrap(err, "connection type")
	}

	if err := m.Signature.Deserialize(r); err != nil {
		return errors.Wrap(err, "signature")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m Register) Serialize(w io.Writer) error {
	if err := binary.Write(w, Endian, m.Version); err != nil {
		return errors.Wrap(err, "version")
	}

	if err := m.Key.Serialize(w); err != nil {
		return errors.Wrap(err, "key")
	}

	if err := m.Hash.Serialize(w); err != nil {
		return errors.Wrap(err, "hash")
	}

	if err := binary.Write(w, Endian, m.StartBlockHeight); err != nil {
		return errors.Wrap(err, "start block height")
	}

	if err := m.ChainTip.Serialize(w); err != nil {
		return errors.Wrap(err, "chain tip")
	}

	if err := binary.Write(w, Endian, m.ConnectionType); err != nil {
		return errors.Wrap(err, "connection type")
	}

	if err := m.Signature.Serialize(w); err != nil {
		return errors.Wrap(err, "signature")
	}

	return nil
}

// SigHash returns the sig hash of the data to sign for this object.
func (m Register) SigHash() (*bitcoin.Hash32, error) {
	hash := sha256.New()

	if err := binary.Write(hash, Endian, m.Version); err != nil {
		return nil, errors.Wrap(err, "version")
	}

	if err := m.Key.Serialize(hash); err != nil {
		return nil, errors.Wrap(err, "key")
	}

	if err := m.Hash.Serialize(hash); err != nil {
		return nil, errors.Wrap(err, "hash")
	}

	if err := binary.Write(hash, Endian, m.StartBlockHeight); err != nil {
		return nil, errors.Wrap(err, "start block height")
	}

	if err := m.ChainTip.Serialize(hash); err != nil {
		return nil, errors.Wrap(err, "chain tip")
	}

	if err := binary.Write(hash, Endian, m.ConnectionType); err != nil {
		return nil, errors.Wrap(err, "connection type")
	}

	h := sha256.Sum256(hash.Sum(nil))
	return bitcoin.NewHash32(h[:])
}

// Type returns the type of the message.
func (m Register) Type() uint64 {
	return MessageTypeRegister
}

// Deserialize reads the message from a reader.
func (m *SubscribeTx) Deserialize(r io.Reader) error {
	if err := m.TxID.Deserialize(r); err != nil {
		return errors.Wrap(err, "txid")
	}

	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "count")
	}

	m.Indexes = make([]uint32, count)
	for i := range m.Indexes {
		index, err := wire.ReadVarInt(r, wire.ProtocolVersion)
		if err != nil {
			return errors.Wrapf(err, "index %d", i)
		}
		m.Indexes[i] = uint32(index)
	}

	return nil
}

// Serialize writes the message to a writer.
func (m SubscribeTx) Serialize(w io.Writer) error {
	if err := m.TxID.Serialize(w); err != nil {
		return errors.Wrap(err, "txid")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.Indexes))); err != nil {
		return errors.Wrap(err, "count")
	}

	for i, index := range m.Indexes {
		if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(index)); err != nil {
			return errors.Wrapf(err, "index %d", i)
		}
	}

	return nil
}

// Type returns the type of the message.
func (m SubscribeTx) Type() uint64 {
	return MessageTypeSubscribeTx
}

// Deserialize reads the message from a reader.
func (m *UnsubscribeTx) Deserialize(r io.Reader) error {
	if err := m.TxID.Deserialize(r); err != nil {
		return errors.Wrap(err, "txid")
	}

	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "count")
	}

	m.Indexes = make([]uint32, count)
	for i := range m.Indexes {
		index, err := wire.ReadVarInt(r, wire.ProtocolVersion)
		if err != nil {
			return errors.Wrapf(err, "index %d", i)
		}
		m.Indexes[i] = uint32(index)
	}

	return nil
}

// Serialize writes the message to a writer.
func (m UnsubscribeTx) Serialize(w io.Writer) error {
	if err := m.TxID.Serialize(w); err != nil {
		return errors.Wrap(err, "txid")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.Indexes))); err != nil {
		return errors.Wrap(err, "count")
	}

	for i, index := range m.Indexes {
		if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(index)); err != nil {
			return errors.Wrapf(err, "index %d", i)
		}
	}

	return nil
}

// Type returns the type of the message.
func (m UnsubscribeTx) Type() uint64 {
	return MessageTypeUnsubscribeTx
}

// Deserialize reads the message from a reader.
func (m *SubscribeOutputs) Deserialize(r io.Reader) error {
	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "count")
	}

	m.Outputs = make([]*wire.OutPoint, count)
	for i := range m.Outputs {
		outpoint := &wire.OutPoint{}
		if err := outpoint.Deserialize(r); err != nil {
			return errors.Wrapf(err, "outpoint %d", i)
		}
		m.Outputs[i] = outpoint
	}

	return nil
}

// Serialize writes the message to a writer.
func (m SubscribeOutputs) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.Outputs))); err != nil {
		return errors.Wrap(err, "count")
	}

	for i, outpoint := range m.Outputs {
		if err := outpoint.Serialize(w); err != nil {
			return errors.Wrapf(err, "outpoint %d", i)
		}
	}

	return nil
}

// Type returns the type of the message.
func (m SubscribeOutputs) Type() uint64 {
	return MessageTypeSubscribeOutputs
}

// Deserialize reads the message from a reader.
func (m *UnsubscribeOutputs) Deserialize(r io.Reader) error {
	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "count")
	}

	m.Outputs = make([]*wire.OutPoint, count)
	for i := range m.Outputs {
		outpoint := &wire.OutPoint{}
		if err := outpoint.Deserialize(r); err != nil {
			return errors.Wrapf(err, "outpoint %d", i)
		}
		m.Outputs[i] = outpoint
	}

	return nil
}

// Serialize writes the message to a writer.
func (m UnsubscribeOutputs) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.Outputs))); err != nil {
		return errors.Wrap(err, "count")
	}

	for i, outpoint := range m.Outputs {
		if err := outpoint.Serialize(w); err != nil {
			return errors.Wrapf(err, "outpoint %d", i)
		}
	}

	return nil
}

// Type returns the type of the message.
func (m UnsubscribeOutputs) Type() uint64 {
	return MessageTypeUnsubscribeOutputs
}

// Deserialize reads the message from a reader.
func (m *SubscribePushData) Deserialize(r io.Reader) error {
	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "count")
	}

	for i := uint64(0); i < count; i++ {
		size, err := wire.ReadVarInt(r, wire.ProtocolVersion)
		if err != nil {
			return errors.Wrap(err, "size")
		}
		b := make([]byte, size)
		if _, err := io.ReadFull(r, b); err != nil {
			return errors.Wrap(err, "push data")
		}
		m.PushDatas = append(m.PushDatas, b)
	}

	return nil
}

// Serialize writes the message to a writer.
func (m SubscribePushData) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.PushDatas))); err != nil {
		return errors.Wrap(err, "count")
	}

	for _, pushData := range m.PushDatas {
		if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(pushData))); err != nil {
			return errors.Wrap(err, "size")
		}
		if _, err := w.Write(pushData); err != nil {
			return errors.Wrap(err, "push data")
		}
	}

	return nil
}

// Type returns the type of the message.
func (m SubscribePushData) Type() uint64 {
	return MessageTypeSubscribePushData
}

// Deserialize reads the message from a reader.
func (m *UnsubscribePushData) Deserialize(r io.Reader) error {
	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "count")
	}

	for i := uint64(0); i < count; i++ {
		size, err := wire.ReadVarInt(r, wire.ProtocolVersion)
		if err != nil {
			return errors.Wrap(err, "size")
		}
		b := make([]byte, size)
		if _, err := io.ReadFull(r, b); err != nil {
			return errors.Wrap(err, "push data")
		}
		m.PushDatas = append(m.PushDatas, b)
	}

	return nil
}

// Serialize writes the message to a writer.
func (m UnsubscribePushData) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.PushDatas))); err != nil {
		return errors.Wrap(err, "count")
	}

	for _, pushData := range m.PushDatas {
		if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(pushData))); err != nil {
			return errors.Wrap(err, "size")
		}
		if _, err := w.Write(pushData); err != nil {
			return errors.Wrap(err, "push data")
		}
	}

	return nil
}

// Type returns the type of the message.
func (m UnsubscribePushData) Type() uint64 {
	return MessageTypeUnsubscribePushData
}

// Deserialize reads the message from a reader.
func (m *SubscribeHeaders) Deserialize(r io.Reader) error {
	return nil
}

// Serialize writes the message to a writer.
func (m SubscribeHeaders) Serialize(w io.Writer) error {
	return nil
}

// Type returns the type of the message.
func (m SubscribeHeaders) Type() uint64 {
	return MessageTypeSubscribeHeaders
}

// Deserialize reads the message from a reader.
func (m *UnsubscribeHeaders) Deserialize(r io.Reader) error {
	return nil
}

// Serialize writes the message to a writer.
func (m UnsubscribeHeaders) Serialize(w io.Writer) error {
	return nil
}

// Type returns the type of the message.
func (m UnsubscribeHeaders) Type() uint64 {
	return MessageTypeUnsubscribeHeaders
}

// Deserialize reads the message from a reader.
func (m *SubscribeContracts) Deserialize(r io.Reader) error {
	return nil
}

// Serialize writes the message to a writer.
func (m SubscribeContracts) Serialize(w io.Writer) error {
	return nil
}

// Type returns the type of the message.
func (m SubscribeContracts) Type() uint64 {
	return MessageTypeSubscribeContracts
}

// Deserialize reads the message from a reader.
func (m *UnsubscribeContracts) Deserialize(r io.Reader) error {
	return nil
}

// Serialize writes the message to a writer.
func (m UnsubscribeContracts) Serialize(w io.Writer) error {
	return nil
}

// Type returns the type of the message.
func (m UnsubscribeContracts) Type() uint64 {
	return MessageTypeUnsubscribeContracts
}

// Deserialize reads the message from a reader.
func (m *Ready) Deserialize(r io.Reader) error {
	id, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "next message id")
	}
	m.NextMessageID = id

	return nil
}

// Serialize writes the message to a writer.
func (m Ready) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.NextMessageID); err != nil {
		return errors.Wrap(err, "next message id")
	}

	return nil
}

// Type returns the type of the message.
func (m Ready) Type() uint64 {
	return MessageTypeReady
}

// Deserialize reads the message from a reader.
func (m *GetChainTip) Deserialize(r io.Reader) error {
	return nil
}

// Serialize writes the message to a writer.
func (m GetChainTip) Serialize(w io.Writer) error {
	return nil
}

// Type returns the type of the message.
func (m GetChainTip) Type() uint64 {
	return MessageTypeGetChainTip
}

// Deserialize reads the message from a reader.
func (m *GetHeaders) Deserialize(r io.Reader) error {
	if err := binary.Read(r, Endian, &m.RequestHeight); err != nil {
		return errors.Wrap(err, "request height")
	}

	maxCount, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "max count")
	}
	m.MaxCount = uint32(maxCount)

	return nil
}

// Serialize writes the message to a writer.
func (m GetHeaders) Serialize(w io.Writer) error {
	if err := binary.Write(w, Endian, m.RequestHeight); err != nil {
		return errors.Wrap(err, "request height")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(m.MaxCount)); err != nil {
		return errors.Wrap(err, "max count")
	}

	return nil
}

// Type returns the type of the message.
func (m GetHeaders) Type() uint64 {
	return MessageTypeGetHeaders
}

// Deserialize reads the message from a reader.
func (m *GetHeader) Deserialize(r io.Reader) error {
	if err := m.BlockHash.Deserialize(r); err != nil {
		return errors.Wrap(err, "block hash")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m GetHeader) Serialize(w io.Writer) error {
	if err := m.BlockHash.Serialize(w); err != nil {
		return errors.Wrap(err, "block hash")
	}

	return nil
}

// Type returns the type of the message.
func (m GetHeader) Type() uint64 {
	return MessageTypeGetHeader
}

// Deserialize reads the message from a reader.
func (m *GetFeeQuotes) Deserialize(r io.Reader) error {
	return nil
}

// Serialize writes the message to a writer.
func (m GetFeeQuotes) Serialize(w io.Writer) error {
	return nil
}

// Type returns the type of the message.
func (m GetFeeQuotes) Type() uint64 {
	return MessageTypeGetFeeQuotes
}

// Deserialize reads the message from a reader.
func (m *SendTx) Deserialize(r io.Reader) error {
	m.Tx = &wire.MsgTx{}
	if err := m.Tx.Deserialize(r); err != nil {
		return errors.Wrap(err, "tx")
	}

	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "count")
	}

	m.Indexes = make([]uint32, count)
	for i := range m.Indexes {
		index, err := wire.ReadVarInt(r, wire.ProtocolVersion)
		if err != nil {
			return errors.Wrapf(err, "index %d", i)
		}
		m.Indexes[i] = uint32(index)
	}

	return nil
}

// Serialize writes the message to a writer.
func (m SendTx) Serialize(w io.Writer) error {
	if err := m.Tx.Serialize(w); err != nil {
		return errors.Wrap(err, "tx")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.Indexes))); err != nil {
		return errors.Wrap(err, "count")
	}

	for i, index := range m.Indexes {
		if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(index)); err != nil {
			return errors.Wrapf(err, "index %d", i)
		}
	}

	return nil
}

// Type returns the type of the message.
func (m SendTx) Type() uint64 {
	return MessageTypeSendTx
}

// Deserialize reads the message from a reader.
func (m *SendExpandedTx) Deserialize(r io.Reader) error {
	txSize, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "tx size")
	}

	script := make(bitcoin.Script, txSize)
	if _, err := io.ReadFull(r, script); err != nil {
		return errors.Wrap(err, "script")
	}

	m.Tx = &expanded_tx.ExpandedTx{}
	if _, err := bsor.UnmarshalBinary(script, m.Tx); err != nil {
		return errors.Wrap(err, "tx")
	}

	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "count")
	}

	m.Indexes = make([]uint32, count)
	for i := range m.Indexes {
		index, err := wire.ReadVarInt(r, wire.ProtocolVersion)
		if err != nil {
			return errors.Wrapf(err, "index %d", i)
		}
		m.Indexes[i] = uint32(index)
	}

	return nil
}

// Serialize writes the message to a writer.
func (m SendExpandedTx) Serialize(w io.Writer) error {
	script, err := bsor.MarshalBinary(m.Tx)
	if err != nil {
		return errors.Wrap(err, "tx")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(script))); err != nil {
		return errors.Wrap(err, "tx size")
	}

	if _, err := w.Write(script); err != nil {
		return errors.Wrap(err, "script")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.Indexes))); err != nil {
		return errors.Wrap(err, "count")
	}

	for i, index := range m.Indexes {
		if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(index)); err != nil {
			return errors.Wrapf(err, "index %d", i)
		}
	}

	return nil
}

// Type returns the type of the message.
func (m SendExpandedTx) Type() uint64 {
	return MessageTypeSendExpandedTx
}

// Deserialize reads the message from a reader.
func (m *SaveTxs) Deserialize(r io.Reader) error {
	txsSize, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "tx size")
	}

	script := make(bitcoin.Script, txsSize)
	if _, err := io.ReadFull(r, script); err != nil {
		return errors.Wrap(err, "script")
	}

	m.Txs = expanded_tx.AncestorTxs{}
	if _, err := bsor.UnmarshalBinary(script, &m.Txs); err != nil {
		return errors.Wrap(err, "txs")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m SaveTxs) Serialize(w io.Writer) error {
	script, err := bsor.MarshalBinary(m.Txs)
	if err != nil {
		return errors.Wrap(err, "tx")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(script))); err != nil {
		return errors.Wrap(err, "tx size")
	}

	if _, err := w.Write(script); err != nil {
		return errors.Wrap(err, "script")
	}

	return nil
}

// Type returns the type of the message.
func (m SaveTxs) Type() uint64 {
	return MessageTypeSaveTxs
}

// Deserialize reads the message from a reader.
func (m *GetTx) Deserialize(r io.Reader) error {
	if err := m.TxID.Deserialize(r); err != nil {
		return errors.Wrap(err, "tx")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m GetTx) Serialize(w io.Writer) error {
	if err := m.TxID.Serialize(w); err != nil {
		return errors.Wrap(err, "tx")
	}

	return nil
}

// Type returns the type of the message.
func (m GetTx) Type() uint64 {
	return MessageTypeGetTx
}

// Deserialize reads the message from a reader.
func (m *ReprocessTx) Deserialize(r io.Reader) error {
	if err := m.TxID.Deserialize(r); err != nil {
		return errors.Wrap(err, "tx")
	}

	clientCount, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "client id count")
	}

	m.ClientIDs = make([]bitcoin.Hash20, clientCount)
	for i := range m.ClientIDs {
		if err := m.ClientIDs[i].Deserialize(r); err != nil {
			return errors.Wrapf(err, "client id %d", i)
		}
	}

	return nil
}

// Serialize writes the message to a writer.
func (m ReprocessTx) Serialize(w io.Writer) error {
	if err := m.TxID.Serialize(w); err != nil {
		return errors.Wrap(err, "tx")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.ClientIDs))); err != nil {
		return errors.Wrap(err, "client id count")
	}

	for i, clientID := range m.ClientIDs {
		if err := clientID.Serialize(w); err != nil {
			return errors.Wrapf(err, "client id %d", i)
		}
	}

	return nil
}

// Type returns the type of the message.
func (m ReprocessTx) Type() uint64 {
	return MessageTypeReprocessTx
}

// Deserialize reads the message from a reader.
func (m *MarkHeaderInvalid) Deserialize(r io.Reader) error {
	if err := m.BlockHash.Deserialize(r); err != nil {
		return errors.Wrap(err, "block_hash")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m MarkHeaderInvalid) Serialize(w io.Writer) error {
	if err := m.BlockHash.Serialize(w); err != nil {
		return errors.Wrap(err, "block_hash")
	}

	return nil
}

// Type returns the type of the message.
func (m MarkHeaderInvalid) Type() uint64 {
	return MessageTypeMarkHeaderInvalid
}

// Deserialize reads the message from a reader.
func (m *MarkHeaderNotInvalid) Deserialize(r io.Reader) error {
	if err := m.BlockHash.Deserialize(r); err != nil {
		return errors.Wrap(err, "block_hash")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m MarkHeaderNotInvalid) Serialize(w io.Writer) error {
	if err := m.BlockHash.Serialize(w); err != nil {
		return errors.Wrap(err, "block_hash")
	}

	return nil
}

// Type returns the type of the message.
func (m MarkHeaderNotInvalid) Type() uint64 {
	return MessageTypeMarkHeaderNotInvalid
}

// Server to Client Messages -----------------------------------------------------------------------

// Deserialize reads the message from a reader.
func (m *AcceptRegister) Deserialize(r io.Reader) error {
	if err := m.Key.Deserialize(r); err != nil {
		return errors.Wrap(err, "key")
	}

	pushDataCount, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "push data count")
	}
	m.PushDataCount = pushDataCount

	utxoCount, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "utxo count")
	}
	m.UTXOCount = utxoCount

	messageCount, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "message count")
	}
	m.MessageCount = messageCount

	if err := m.Signature.Deserialize(r); err != nil {
		return errors.Wrap(err, "signature")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m AcceptRegister) Serialize(w io.Writer) error {
	if err := m.Key.Serialize(w); err != nil {
		return errors.Wrap(err, "key")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.PushDataCount); err != nil {
		return errors.Wrap(err, "push data count")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.UTXOCount); err != nil {
		return errors.Wrap(err, "utxo count")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.MessageCount); err != nil {
		return errors.Wrap(err, "message count")
	}

	if err := m.Signature.Serialize(w); err != nil {
		return errors.Wrap(err, "signature")
	}

	return nil
}

// Type returns the type of the message.
func (m AcceptRegister) Type() uint64 {
	return MessageTypeAcceptRegister
}

// SigHash returns the signature hash to use to sign and check the signature.
func (m AcceptRegister) SigHash(h bitcoin.Hash32) (*bitcoin.Hash32, error) {
	hash := sha256.New()

	if err := m.Key.Serialize(hash); err != nil {
		return nil, errors.Wrap(err, "key")
	}

	if err := wire.WriteVarInt(hash, wire.ProtocolVersion, m.PushDataCount); err != nil {
		return nil, errors.Wrap(err, "push data count")
	}

	if err := wire.WriteVarInt(hash, wire.ProtocolVersion, m.UTXOCount); err != nil {
		return nil, errors.Wrap(err, "utxo count")
	}

	if err := wire.WriteVarInt(hash, wire.ProtocolVersion, m.MessageCount); err != nil {
		return nil, errors.Wrap(err, "message count")
	}

	if err := h.Serialize(hash); err != nil {
		return nil, errors.Wrap(err, "key")
	}

	hs := sha256.Sum256(hash.Sum(nil))
	return bitcoin.NewHash32(hs[:])
}

// Deserialize reads the message from a reader.
func (m *Tx) Deserialize(r io.Reader) error {

	id, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "id")
	}
	m.ID = id

	m.Tx = &wire.MsgTx{}
	if err := m.Tx.Deserialize(r); err != nil {
		return errors.Wrap(err, "tx")
	}

	m.Outputs = make([]*wire.TxOut, len(m.Tx.TxIn))
	for i := range m.Tx.TxIn {
		output := &wire.TxOut{}
		if err := output.Deserialize(r, wire.ProtocolVersion, m.Tx.Version); err != nil {
			return errors.Wrap(err, "output")
		}
		m.Outputs[i] = output
	}

	if err := m.State.Deserialize(r); err != nil {
		return errors.Wrap(err, "state")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m Tx) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.ID); err != nil {
		return errors.Wrap(err, "id")
	}

	if err := m.Tx.Serialize(w); err != nil {
		return errors.Wrap(err, "tx")
	}

	for _, output := range m.Outputs {
		if err := output.Serialize(w, wire.ProtocolVersion, m.Tx.Version); err != nil {
			return errors.Wrap(err, "output")
		}
	}

	if err := m.State.Serialize(w); err != nil {
		return errors.Wrap(err, "state")
	}

	return nil
}

// Type returns the type of the message.
func (m Tx) Type() uint64 {
	return MessageTypeTx
}

// Deserialize reads the message from a reader.
func (m *BaseTx) Deserialize(r io.Reader) error {
	m.Tx = &wire.MsgTx{}
	if err := m.Tx.Deserialize(r); err != nil {
		return errors.Wrap(err, "tx")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m BaseTx) Serialize(w io.Writer) error {
	if err := m.Tx.Serialize(w); err != nil {
		return errors.Wrap(err, "tx")
	}

	return nil
}

// Type returns the type of the message.
func (m BaseTx) Type() uint64 {
	return MessageTypeBaseTx
}

// Deserialize reads the message from a reader.
func (m *TxUpdate) Deserialize(r io.Reader) error {
	id, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "id")
	}
	m.ID = id

	if err := m.TxID.Deserialize(r); err != nil {
		return errors.Wrap(err, "txid")
	}

	if err := m.State.Deserialize(r); err != nil {
		return errors.Wrap(err, "state")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m TxUpdate) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.ID); err != nil {
		return errors.Wrap(err, "id")
	}

	if err := m.TxID.Serialize(w); err != nil {
		return errors.Wrap(err, "txid")
	}

	if err := m.State.Serialize(w); err != nil {
		return errors.Wrap(err, "state")
	}

	return nil
}

// Type returns the type of the message.
func (m TxUpdate) Type() uint64 {
	return MessageTypeTxUpdate
}

// Deserialize reads the message from a reader.
func (m *Headers) Deserialize(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &m.RequestHeight); err != nil {
		return errors.Wrap(err, "request height")
	}

	startHeight, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "start height")
	}
	m.StartHeight = uint32(startHeight)

	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "count")
	}

	m.Headers = make([]*wire.BlockHeader, count)
	for i := range m.Headers {
		header := &wire.BlockHeader{}
		if err := header.Deserialize(r); err != nil {
			return errors.Wrapf(err, "header %d", i)
		}
		m.Headers[i] = header
	}

	return nil
}

// Serialize writes the message to a writer.
func (m Headers) Serialize(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, m.RequestHeight); err != nil {
		return errors.Wrap(err, "request height")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(m.StartHeight)); err != nil {
		return errors.Wrap(err, "start height")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.Headers))); err != nil {
		return errors.Wrap(err, "count")
	}

	for i, header := range m.Headers {
		if err := header.Serialize(w); err != nil {
			return errors.Wrapf(err, "header %d", i)
		}
	}

	return nil
}

// Type returns the type of the message.
func (m Headers) Type() uint64 {
	return MessageTypeHeaders
}

// Deserialize reads the message from a reader.
func (m *Header) Deserialize(r io.Reader) error {
	if err := m.Header.Deserialize(r); err != nil {
		return errors.Wrap(err, "header")
	}

	blockHeight, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "block height")
	}
	m.BlockHeight = uint32(blockHeight)

	if err := binary.Read(r, Endian, &m.IsMostPOW); err != nil {
		return errors.Wrap(err, "is most pow")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m Header) Serialize(w io.Writer) error {
	if err := m.Header.Serialize(w); err != nil {
		return errors.Wrap(err, "header")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(m.BlockHeight)); err != nil {
		return errors.Wrap(err, "block height")
	}

	if err := binary.Write(w, Endian, m.IsMostPOW); err != nil {
		return errors.Wrap(err, "is most pow")
	}

	return nil
}

// Type returns the type of the message.
func (m Header) Type() uint64 {
	return MessageTypeHeader
}

// Deserialize reads the message from a reader.
func (m *FeeQuotes) Deserialize(r io.Reader) error {
	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "count")
	}

	m.FeeQuotes = make(merchant_api.FeeQuotes, count)
	for i := range m.FeeQuotes {
		feeQuote, err := DeserializeFeeQuote(r)
		if err != nil {
			return errors.Wrapf(err, "fee quote %d", i)
		}
		m.FeeQuotes[i] = feeQuote
	}

	return nil
}

// Serialize writes the message to a writer.
func (m FeeQuotes) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.FeeQuotes))); err != nil {
		return errors.Wrap(err, "count")
	}

	for i, feeQuote := range m.FeeQuotes {
		if err := SerializeFeeQuote(feeQuote, w); err != nil {
			return errors.Wrapf(err, "fee quote %d", i)
		}
	}

	return nil
}

// Type returns the type of the message.
func (m FeeQuotes) Type() uint64 {
	return MessageTypeFeeQuotes
}

func DeserializeFeeQuote(r io.Reader) (*merchant_api.FeeQuote, error) {
	var feeType uint8
	if err := binary.Read(r, Endian, &feeType); err != nil {
		return nil, errors.Wrap(err, "fee type")
	}

	miningFee, err := DeserializeFee(r)
	if err != nil {
		return nil, errors.Wrap(err, "mining fee")
	}

	relayFee, err := DeserializeFee(r)
	if err != nil {
		return nil, errors.Wrap(err, "relay fee")
	}

	return &merchant_api.FeeQuote{
		FeeType:   merchant_api.FeeType(feeType),
		MiningFee: *miningFee,
		RelayFee:  *relayFee,
	}, nil
}

func SerializeFeeQuote(feeQuote *merchant_api.FeeQuote, w io.Writer) error {
	feeType := uint8(feeQuote.FeeType)
	if err := binary.Write(w, Endian, feeType); err != nil {
		return errors.Wrap(err, "fee type")
	}

	if err := SerializeFee(feeQuote.MiningFee, w); err != nil {
		return errors.Wrap(err, "mining fee")
	}

	if err := SerializeFee(feeQuote.RelayFee, w); err != nil {
		return errors.Wrap(err, "relay fee")
	}

	return nil
}

func DeserializeFee(r io.Reader) (*merchant_api.Fee, error) {
	satoshis, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return nil, errors.Wrap(err, "satoshis")
	}

	bytes, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return nil, errors.Wrap(err, "bytes")
	}

	return &merchant_api.Fee{
		Satoshis: satoshis,
		Bytes:    bytes,
	}, nil
}

func SerializeFee(fee merchant_api.Fee, w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, fee.Satoshis); err != nil {
		return errors.Wrap(err, "satoshis")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, fee.Bytes); err != nil {
		return errors.Wrap(err, "bytes")
	}

	return nil
}

// Deserialize reads the message from a reader.
func (m *PostMerkleProofs) Deserialize(r io.Reader) error {
	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "count")
	}

	m.MerkleProofs = make([]*merkle_proof.MerkleProof, count)
	for i := range m.MerkleProofs {
		merkleProof := &merkle_proof.MerkleProof{}
		if err := merkleProof.Deserialize(r); err != nil {
			return errors.Wrapf(err, "merkle proof %d", i)
		}
		m.MerkleProofs[i] = merkleProof
	}

	return nil
}

// Serialize writes the message to a writer.
func (m PostMerkleProofs) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.MerkleProofs))); err != nil {
		return errors.Wrap(err, "count")
	}

	for i, merkleProof := range m.MerkleProofs {
		if err := merkleProof.Serialize(w); err != nil {
			return errors.Wrapf(err, "merkle proof %d", i)
		}
	}

	return nil
}

// Type returns the type of the message.
func (m PostMerkleProofs) Type() uint64 {
	return MessageTypePostMerkleProofs
}

// Deserialize reads the message from a reader.
func (m *InSync) Deserialize(r io.Reader) error {
	return nil
}

// Serialize writes the message to a writer.
func (m InSync) Serialize(w io.Writer) error {
	return nil
}

// Type returns the type of the message.
func (m InSync) Type() uint64 {
	return MessageTypeInSync
}

// Deserialize reads the message from a reader.
func (m *ChainTip) Deserialize(r io.Reader) error {
	height, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "height")
	}
	m.Height = uint32(height)

	if err := m.Hash.Deserialize(r); err != nil {
		return errors.Wrap(err, "hash")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m ChainTip) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(m.Height)); err != nil {
		return errors.Wrap(err, "height")
	}

	if err := m.Hash.Serialize(w); err != nil {
		return errors.Wrap(err, "hash")
	}

	return nil
}

// Type returns the type of the message.
func (m ChainTip) Type() uint64 {
	return MessageTypeChainTip
}

// Deserialize reads the message from a reader.
func (m *Reject) Deserialize(r io.Reader) error {
	t, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "message type")
	}
	m.MessageType = t

	var hashIncluded bool
	if err := binary.Read(r, Endian, &hashIncluded); err != nil {
		return errors.Wrap(err, "hash included")
	}

	if hashIncluded {
		m.Hash = &bitcoin.Hash32{}
		if err := m.Hash.Deserialize(r); err != nil {
			return errors.Wrap(err, "hash")
		}
	}

	d, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "code")
	}
	m.Code = RejectCode(d)

	length, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "message length")
	}

	b := make([]byte, length)
	if _, err := io.ReadFull(r, b); err != nil {
		return errors.Wrap(err, "message")
	}
	m.Message = string(b)

	return nil
}

// Serialize writes the message to a writer.
func (m Reject) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.MessageType); err != nil {
		return errors.Wrap(err, "message type")
	}

	if m.Hash != nil {
		if err := binary.Write(w, Endian, true); err != nil {
			return errors.Wrap(err, "hash included")
		}

		if err := m.Hash.Serialize(w); err != nil {
			return errors.Wrap(err, "hash")
		}
	} else {
		if err := binary.Write(w, Endian, false); err != nil {
			return errors.Wrap(err, "hash included")
		}
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(m.Code)); err != nil {
		return errors.Wrap(err, "code")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.Message))); err != nil {
		return errors.Wrap(err, "message length")
	}

	if _, err := w.Write([]byte(m.Message)); err != nil {
		return errors.Wrap(err, "message")
	}

	return nil
}

// Type returns the type of the message.
func (m Reject) Type() uint64 {
	return MessageTypeReject
}

// Deserialize reads the message from a reader.
func (m *Ping) Deserialize(r io.Reader) error {
	t, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "timestamp")
	}
	m.TimeStamp = t

	return nil
}

// Serialize writes the message to a writer.
func (m Ping) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.TimeStamp); err != nil {
		return errors.Wrap(err, "timestamp")
	}

	return nil
}

// Type returns the type of the message.
func (m Ping) Type() uint64 {
	return MessageTypePing
}

// Deserialize reads the message from a reader.
func (m *Pong) Deserialize(r io.Reader) error {
	t, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "request timestamp")
	}
	m.RequestTimeStamp = t

	t, err = wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "timestamp")
	}
	m.TimeStamp = t

	return nil
}

// Serialize writes the message to a writer.
func (m Pong) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.RequestTimeStamp); err != nil {
		return errors.Wrap(err, "request timestamp")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.TimeStamp); err != nil {
		return errors.Wrap(err, "timestamp")
	}

	return nil
}

// Type returns the type of the message.
func (m Pong) Type() uint64 {
	return MessageTypePong
}

// Deserialize reads the message from a reader.
func (m *Accept) Deserialize(r io.Reader) error {
	t, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "message type")
	}
	m.MessageType = t

	var hashIncluded bool
	if err := binary.Read(r, Endian, &hashIncluded); err != nil {
		return errors.Wrap(err, "hash included")
	}

	if hashIncluded {
		m.Hash = &bitcoin.Hash32{}
		if err := m.Hash.Deserialize(r); err != nil {
			return errors.Wrap(err, "hash")
		}
	}

	return nil
}

// Serialize writes the message to a writer.
func (m Accept) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.MessageType); err != nil {
		return errors.Wrap(err, "message type")
	}

	if m.Hash != nil {
		if err := binary.Write(w, Endian, true); err != nil {
			return errors.Wrap(err, "hash included")
		}

		if err := m.Hash.Serialize(w); err != nil {
			return errors.Wrap(err, "hash")
		}
	} else {
		if err := binary.Write(w, Endian, false); err != nil {
			return errors.Wrap(err, "hash included")
		}
	}

	return nil
}

// Type returns the type of the message.
func (m Accept) Type() uint64 {
	return MessageTypeAccept
}

// Deserialize reads the message from a reader.
func (m *TxState) Deserialize(r io.Reader) error {
	if err := binary.Read(r, Endian, &m.Safe); err != nil {
		return errors.Wrap(err, "safe")
	}

	if err := binary.Read(r, Endian, &m.UnSafe); err != nil {
		return errors.Wrap(err, "unsafe")
	}

	if err := binary.Read(r, Endian, &m.Cancelled); err != nil {
		return errors.Wrap(err, "cancelled")
	}

	if err := binary.Read(r, Endian, &m.UnconfirmedDepth); err != nil {
		return errors.Wrap(err, "unconfirmed depth")
	}

	var merkleProofIncluded bool
	if err := binary.Read(r, Endian, &merkleProofIncluded); err != nil {
		return errors.Wrap(err, "merkle proof included")
	}

	if !merkleProofIncluded {
		m.MerkleProof = nil
		return nil
	}

	m.MerkleProof = &MerkleProof{}
	if err := m.MerkleProof.Deserialize(r); err != nil {
		return errors.Wrap(err, "merkle proof")
	}

	return nil
}

// Serialize writes the message to a writer.
func (m TxState) Serialize(w io.Writer) error {
	if err := binary.Write(w, Endian, m.Safe); err != nil {
		return errors.Wrap(err, "safe")
	}

	if err := binary.Write(w, Endian, m.UnSafe); err != nil {
		return errors.Wrap(err, "unsafe")
	}

	if err := binary.Write(w, Endian, m.Cancelled); err != nil {
		return errors.Wrap(err, "cancelled")
	}

	if err := binary.Write(w, Endian, m.UnconfirmedDepth); err != nil {
		return errors.Wrap(err, "unconfirmed depth")
	}

	if m.MerkleProof == nil {
		if err := binary.Write(w, Endian, false); err != nil {
			return errors.Wrap(err, "merkle proof included")
		}

		return nil
	}

	if err := binary.Write(w, Endian, true); err != nil {
		return errors.Wrap(err, "merkle proof included")
	}

	if err := m.MerkleProof.Serialize(w); err != nil {
		return errors.Wrap(err, "merkle proof")
	}

	return nil
}

// Deserialize reads the message from a reader.
func (m *MerkleProof) Deserialize(r io.Reader) error {
	var err error
	m.Index, err = wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "index")
	}

	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "layer count")
	}
	m.Path = make([]bitcoin.Hash32, count)
	for i := uint64(0); i < count; i++ {
		if err := m.Path[i].Deserialize(r); err != nil {
			return errors.Wrap(err, "hash")
		}
	}

	if err := m.BlockHeader.Deserialize(r); err != nil {
		return errors.Wrap(err, "block header")
	}

	count, err = wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "duplicate index count")
	}
	m.DuplicatedIndexes = make([]uint64, count)
	for i := uint64(0); i < count; i++ {
		index, err := wire.ReadVarInt(r, wire.ProtocolVersion)
		if err != nil {
			return errors.Wrap(err, "duplicate index")
		}
		m.DuplicatedIndexes[i] = index
	}

	return nil
}

// Serialize writes the message to a writer.
func (m MerkleProof) Serialize(w io.Writer) error {
	if err := wire.WriteVarInt(w, wire.ProtocolVersion, m.Index); err != nil {
		return errors.Wrap(err, "index")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.Path))); err != nil {
		return errors.Wrap(err, "path count")
	}
	for _, hash := range m.Path {
		if err := hash.Serialize(w); err != nil {
			return errors.Wrap(err, "hash")
		}
	}

	if err := m.BlockHeader.Serialize(w); err != nil {
		return errors.Wrap(err, "block header")
	}

	if err := wire.WriteVarInt(w, wire.ProtocolVersion, uint64(len(m.DuplicatedIndexes))); err != nil {
		return errors.Wrap(err, "duplicate index count")
	}
	for _, index := range m.DuplicatedIndexes {
		if err := wire.WriteVarInt(w, wire.ProtocolVersion, index); err != nil {
			return errors.Wrap(err, "duplicate index")
		}
	}

	return nil
}

// IsValid returns nil if the merkle proof is valid.
// If version is odd then the TxID must be set because it is not included in serialization.
// If version is 0x00, 0x01 (block hash serialized) or 0x06, 0x07 (no last element) then the
// MerkleRoot or BlockHeader must be set.
func (m MerkleProof) IsValid(txid bitcoin.Hash32) error {
	index := m.Index
	layer := uint64(1)
	hash := txid
	path := m.Path
	duplicateIndexes := m.DuplicatedIndexes

	for {
		isLeft := index%2 == 0

		// Check duplicate index
		var otherHash bitcoin.Hash32
		if len(duplicateIndexes) > 0 && layer == duplicateIndexes[0] {
			otherHash = hash
			duplicateIndexes = duplicateIndexes[1:]
		} else {
			if len(path) == 0 {
				break
			}
			otherHash = path[0]
			path = path[1:]
		}

		if otherHash.Equal(&hash) && !isLeft {
			// Left hash can't be duplicate
			return errors.Wrap(ErrInvalid, "left node is duplicate")
		}

		s := sha256.New()
		if isLeft {
			s.Write(hash[:])
			s.Write(otherHash[:])
		} else {
			s.Write(otherHash[:])
			s.Write(hash[:])
		}
		hash = sha256.Sum256(s.Sum(nil))

		index = index / 2
		layer++
	}

	if hash.Equal(&m.BlockHeader.MerkleRoot) {
		return nil
	}

	return ErrWrongHash
}

// ConvertWireMerkleProof converts from a wire merkle proof.
func ConvertWireMerkleProof(mp *wire.MerkleProof) *MerkleProof {
	result := &MerkleProof{
		Index:       uint64(mp.Index),
		Path:        mp.Path,
		BlockHeader: *mp.BlockHeader,
	}

	result.DuplicatedIndexes = make([]uint64, len(mp.DuplicatedIndexes))
	for i, di := range mp.DuplicatedIndexes {
		result.DuplicatedIndexes[i] = uint64(di)
	}

	return result
}

func ConvertMerkleProof(mp *merkle_proof.MerkleProof) *MerkleProof {
	result := &MerkleProof{
		Index:       uint64(mp.Index),
		Path:        mp.Path,
		BlockHeader: *mp.BlockHeader,
	}

	result.DuplicatedIndexes = make([]uint64, len(mp.DuplicatedIndexes))
	for i, di := range mp.DuplicatedIndexes {
		result.DuplicatedIndexes[i] = uint64(di)
	}

	return result
}

func (mp *MerkleProof) ConvertToMerkleProof(txid bitcoin.Hash32) *merkle_proof.MerkleProof {
	result := &merkle_proof.MerkleProof{
		Index:             int(mp.Index),
		TxID:              &txid,
		Path:              mp.Path,
		BlockHeader:       &mp.BlockHeader,
		DuplicatedIndexes: make([]int, len(mp.DuplicatedIndexes)),
	}

	for i, di := range mp.DuplicatedIndexes {
		result.DuplicatedIndexes[i] = int(di)
	}

	return result
}
