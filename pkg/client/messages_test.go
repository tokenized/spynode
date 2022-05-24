package client

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/merchant_api"
	"github.com/tokenized/pkg/wire"
)

func Test_SerializeMessages(t *testing.T) {
	k, err := bitcoin.GenerateKey(bitcoin.MainNet)
	if err != nil {
		t.Fatalf("Failed to generate key : %s", err)
	}

	pk := k.PublicKey()

	var sigHash bitcoin.Hash32
	rand.Read(sigHash[:])

	sig, err := k.Sign(sigHash)
	if err != nil {
		t.Fatalf("Failed to sign : %s", err)
	}

	var hash bitcoin.Hash32
	rand.Read(hash[:])

	var hash20 bitcoin.Hash20
	rand.Read(hash20[:])

	tx := wire.NewMsgTx(1)

	unlockingScript := make([]byte, 134)
	rand.Read(unlockingScript)
	tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&hash, 0), unlockingScript))

	lockingScript := make([]byte, 34)
	rand.Read(lockingScript)
	txout := wire.NewTxOut(1039, lockingScript)

	tm := uint32(time.Now().Unix())

	var messages = []struct {
		name string
		t    uint64
		n    string
		m    MessagePayload
	}{
		{
			name: "Register",
			n:    "register",
			t:    MessageTypeRegister,
			m: &Register{
				Version:          1,
				Key:              pk,
				Hash:             hash,
				StartBlockHeight: 12345,
				ChainTip:         hash,
				Signature:        sig,
			},
		},
		{
			name: "SubscribePushData",
			n:    "subscribe_push_data",
			t:    MessageTypeSubscribePushData,
			m: &SubscribePushData{
				PushDatas: [][]byte{
					sigHash[:],
					hash.Bytes(),
				},
			},
		},
		{
			name: "UnsubscribePushData",
			n:    "unsubscribe_push_data",
			t:    MessageTypeUnsubscribePushData,
			m: &UnsubscribePushData{
				PushDatas: [][]byte{
					sigHash[:],
					hash.Bytes(),
				},
			},
		},
		{
			name: "SubscribeTx",
			n:    "subscribe_tx",
			t:    MessageTypeSubscribeTx,
			m: &SubscribeTx{
				TxID:    hash,
				Indexes: []uint32{0},
			},
		},
		{
			name: "UnsubscribeTx",
			n:    "unsubscribe_tx",
			t:    MessageTypeUnsubscribeTx,
			m: &UnsubscribeTx{
				TxID:    hash,
				Indexes: []uint32{0},
			},
		},
		{
			name: "SubscribeOutputs",
			n:    "subscribe_outputs",
			t:    MessageTypeSubscribeOutputs,
			m: &SubscribeOutputs{
				Outputs: []*wire.OutPoint{
					&wire.OutPoint{
						Hash:  hash,
						Index: 0,
					},
				},
			},
		},
		{
			name: "UnsubscribeOutputs",
			n:    "unsubscribe_outputs",
			t:    MessageTypeUnsubscribeOutputs,
			m: &UnsubscribeOutputs{
				Outputs: []*wire.OutPoint{
					&wire.OutPoint{
						Hash:  hash,
						Index: 0,
					},
				},
			},
		},
		{
			name: "SubscribeHeaders",
			n:    "subscribe_headers",
			t:    MessageTypeSubscribeHeaders,
			m:    &SubscribeHeaders{},
		},
		{
			name: "UnsubscribeHeaders",
			n:    "unsubscribe_headers",
			t:    MessageTypeUnsubscribeHeaders,
			m:    &UnsubscribeHeaders{},
		},
		{
			name: "SubscribeContracts",
			n:    "subscribe_contracts",
			t:    MessageTypeSubscribeContracts,
			m:    &SubscribeContracts{},
		},
		{
			name: "UnsubscribeContracts",
			n:    "unsubscribe_contracts",
			t:    MessageTypeUnsubscribeContracts,
			m:    &UnsubscribeContracts{},
		},
		{
			name: "Ready",
			n:    "ready",
			t:    MessageTypeReady,
			m: &Ready{
				NextMessageID: 123,
			},
		},
		{
			name: "GetChainTip",
			n:    "get_chain_tip",
			t:    MessageTypeGetChainTip,
			m:    &GetChainTip{},
		},
		{
			name: "GetHeaders",
			n:    "get_headers",
			t:    MessageTypeGetHeaders,
			m: &GetHeaders{
				RequestHeight: -1,
				MaxCount:      1000,
			},
		},
		{
			name: "SendTx",
			n:    "send_tx",
			t:    MessageTypeSendTx,
			m: &SendTx{
				Tx:      tx,
				Indexes: []uint32{0},
			},
		},
		{
			name: "GetTx",
			n:    "get_tx",
			t:    MessageTypeGetTx,
			m: &GetTx{
				TxID: hash,
			},
		},
		{
			name: "GetHeader",
			n:    "get_header",
			t:    MessageTypeGetHeader,
			m: &GetHeader{
				BlockHash: hash,
			},
		},
		{
			name: "GetFeeQuotes",
			n:    "get_fee_quotes",
			t:    MessageTypeGetFeeQuotes,
			m:    &GetFeeQuotes{},
		},
		{
			name: "ReprocessTx",
			n:    "reprocess_tx",
			t:    MessageTypeReprocessTx,
			m: &ReprocessTx{
				TxID:      hash,
				ClientIDs: []bitcoin.Hash20{hash20},
			},
		},
		{
			name: "MarkHeaderInvalid",
			n:    "mark_header_invalid",
			t:    MessageTypeMarkHeaderInvalid,
			m: &MarkHeaderInvalid{
				BlockHash: hash,
			},
		},
		{
			name: "MarkHeaderNotInvalid",
			n:    "mark_header_not_invalid",
			t:    MessageTypeMarkHeaderNotInvalid,
			m: &MarkHeaderNotInvalid{
				BlockHash: hash,
			},
		},
		{
			name: "AcceptRegister",
			n:    "accept_register",
			t:    MessageTypeAcceptRegister,
			m: &AcceptRegister{
				Key:           pk,
				PushDataCount: 3,
				UTXOCount:     40,
				MessageCount:  1050,
				Signature:     sig,
			},
		},
		{
			name: "BaseTx",
			n:    "base_tx",
			t:    MessageTypeBaseTx,
			m: &BaseTx{
				Tx: tx,
			},
		},
		{
			name: "Tx",
			n:    "tx",
			t:    MessageTypeTx,
			m: &Tx{
				ID:      3938472,
				Tx:      tx,
				Outputs: []*wire.TxOut{txout},
				State: TxState{
					Safe:             true,
					UnSafe:           true,
					Cancelled:        true,
					UnconfirmedDepth: 1,
				},
			},
		},
		{
			name: "TxUpdate",
			n:    "tx_update",
			t:    MessageTypeTxUpdate,
			m: &TxUpdate{
				ID:   3938472,
				TxID: hash,
				State: TxState{
					Safe:             true,
					UnSafe:           true,
					Cancelled:        true,
					UnconfirmedDepth: 1,
					MerkleProof: &MerkleProof{
						Index: 1,
						Path:  []bitcoin.Hash32{hash, hash},
						BlockHeader: wire.BlockHeader{
							Timestamp: tm,
						},
						DuplicatedIndexes: []uint64{},
					},
				},
			},
		},
		{
			name: "Headers",
			n:    "headers",
			t:    MessageTypeHeaders,
			m: &Headers{
				RequestHeight: -1,
				StartHeight:   0,
				Headers: []*wire.BlockHeader{
					&wire.BlockHeader{
						Timestamp: tm,
					},
					&wire.BlockHeader{
						Timestamp: tm,
					},
				},
			},
		},
		{
			name: "Header",
			n:    "header",
			t:    MessageTypeHeader,
			m: &Header{
				Header: wire.BlockHeader{
					Timestamp: tm,
				},
			},
		},
		{
			name: "FeeQuotes",
			n:    "fee_quotes",
			t:    MessageTypeFeeQuotes,
			m: &FeeQuotes{
				FeeQuotes: merchant_api.FeeQuotes{
					{
						FeeType: merchant_api.FeeTypeStandard,
						MiningFee: merchant_api.Fee{
							Satoshis: 500,
							Bytes:    1000,
						},
						RelayFee: merchant_api.Fee{
							Satoshis: 250,
							Bytes:    1000,
						},
					},
					{
						FeeType: merchant_api.FeeTypeData,
						MiningFee: merchant_api.Fee{
							Satoshis: 100,
							Bytes:    1000,
						},
						RelayFee: merchant_api.Fee{
							Satoshis: 50,
							Bytes:    1000,
						},
					},
				},
			},
		},
		{
			name: "InSync",
			n:    "in_sync",
			t:    MessageTypeInSync,
			m:    &InSync{},
		},
		{
			name: "ChainTip",
			n:    "chain_tip",
			t:    MessageTypeChainTip,
			m: &ChainTip{
				Height: 1000,
				Hash:   hash,
			},
		},
		{
			name: "Accept",
			n:    "accept",
			t:    MessageTypeAccept,
			m: &Accept{
				MessageType: MessageTypeSendTx,
				Hash:        &hash,
			},
		},
		{
			name: "Accept (No hash)",
			n:    "accept",
			t:    MessageTypeAccept,
			m: &Accept{
				MessageType: MessageTypeSendTx,
				Hash:        nil,
			},
		},
		{
			name: "Reject",
			n:    "reject",
			t:    MessageTypeReject,
			m: &Reject{
				MessageType: MessageTypeSendTx,
				Hash:        &hash,
				Code:        1,
				Message:     "test reject",
			},
		},
		{
			name: "Reject (No hash)",
			n:    "reject",
			t:    MessageTypeReject,
			m: &Reject{
				MessageType: MessageTypeSendTx,
				Hash:        nil,
				Code:        1,
				Message:     "test reject",
			},
		},
		{
			name: "Ping",
			n:    "ping",
			t:    MessageTypePing,
			m: &Ping{
				TimeStamp: uint64(time.Now().UnixNano()),
			},
		},
		{
			name: "Pong",
			n:    "pong",
			t:    MessageTypePong,
			m: &Pong{
				RequestTimeStamp: uint64(time.Now().UnixNano()),
				TimeStamp:        uint64(time.Now().UnixNano()),
			},
		},
	}

	for _, tt := range messages {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := tt.m.Serialize(&buf); err != nil {
				t.Fatalf("Failed to serialize : %s", err)
			}

			if tt.m.Type() != tt.t {
				t.Fatalf("Wrong type : got %d, want %d", tt.m.Type(), tt.t)
			}

			name, exists := MessageTypeNames[tt.t]
			if !exists {
				t.Fatalf("Type does not exist in names map : %d", tt.t)
			}

			if name != tt.n {
				t.Fatalf("Wrong name : got %s, want %s", name, tt.n)
			}

			read := PayloadForType(tt.t)

			if read == nil {
				t.Fatalf("No payload structure for type : %d", tt.t)
			}

			if err := read.Deserialize(&buf); err != nil {
				t.Fatalf("Failed to deserialize : %s", err)
			}

			if !reflect.DeepEqual(tt.m, read) {
				t.Fatalf("Deserialize not equal : \n  got  %+v\n  want %+v", read, tt.m)
			}

			js, _ := json.MarshalIndent(read, "", "  ")
			t.Logf("Message (%s) : %s", name, js)
		})
	}
}
