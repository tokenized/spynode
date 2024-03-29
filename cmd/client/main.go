package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tokenized/config"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/spynode/pkg/client"
)

var (
	buildVersion = "unknown"
	buildDate    = "unknown"
	buildUser    = "unknown"
)

type Config struct {
	Net     bitcoin.Network
	SpyNode client.Config
}

func main() {
	// -------------------------------------------------------------------------
	// Logging

	logPath := os.Getenv("LOG_FILE_PATH")

	logConfig := logger.NewConfig(strings.ToUpper(os.Getenv("DEVELOPMENT")) == "TRUE",
		strings.ToUpper(os.Getenv("LOG_FORMAT")) == "TEXT", logPath)

	ctx := logger.ContextWithLogConfig(context.Background(), logConfig)

	// -------------------------------------------------------------------------
	// Config

	cfg := &client.Config{}
	if err := config.LoadConfig(ctx, cfg); err != nil {
		logger.Fatal(ctx, "Failed to load config : %s", err)
	}

	maskedConfig, err := config.MarshalJSONMaskedRaw(cfg)
	if err != nil {
		logger.Fatal(ctx, "Failed to marshal config : %s", err)
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.JSON("config", maskedConfig),
	}, "Config")

	spyNode, err := client.NewRemoteClient(cfg)
	if err != nil {
		logger.Fatal(ctx, "Failed to create spynode client : %s", err)
	}

	handler := NewHandler(spyNode)
	spyNode.RegisterHandler(handler)

	var wait sync.WaitGroup
	spyNodeInterrupt := make(chan interface{})
	spyNodeComplete := make(chan interface{})
	var spyNodeErr error
	go func() {
		spyNodeErr = spyNode.Run(ctx, spyNodeInterrupt)
		close(spyNodeComplete)
	}()

	select {
	case <-handler.GetReadyChannel():
		fmt.Printf("SpyNode ready\n")

	case <-time.After(time.Second * 5):
		fmt.Printf("Timed out waiting for ready\n")
		close(spyNodeInterrupt)
		wait.Wait()
		return
	}

	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "send_tx":
			SendTx(ctx, spyNode, os.Args[2:])
			close(spyNodeInterrupt)
			wait.Wait()
			return

		case "subscribe":
			Subscribe(ctx, spyNode, os.Args[2:])
			close(spyNodeInterrupt)
			wait.Wait()
			return

		case "mark_header_invalid":
			MarkHeaderInvalid(ctx, spyNode, os.Args[2:])
			close(spyNodeInterrupt)
			wait.Wait()
			return

		case "mark_header_not_invalid":
			MarkHeaderNotInvalid(ctx, spyNode, os.Args[2:])
			close(spyNodeInterrupt)
			wait.Wait()
			return

		default:
			fmt.Printf("Unknown command : %s\n", os.Args[1])
			close(spyNodeInterrupt)
			wait.Wait()
			return
		}
	}

	fmt.Printf("Listening to spynode\n")
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM)

	select {
	case <-spyNodeComplete:
		fmt.Printf("SpyNode completed : %s\n", spyNodeErr)

	case <-osSignals:
		fmt.Printf("\nShutdown requested\n")
	}

	close(spyNodeInterrupt)
	wait.Wait()
}

func SendTx(ctx context.Context, spyNode *client.RemoteClient, args []string) {
	if len(args) != 1 {
		logger.Fatal(ctx, "Wrong argument count: send_tx [Tx Hex]")
	}

	txBytes, err := hex.DecodeString(args[0])
	if err != nil {
		logger.Fatal(ctx, "Failed to decode tx hex : %s", err)
	}

	tx := &wire.MsgTx{}
	if err := tx.Deserialize(bytes.NewReader(txBytes)); err != nil {
		logger.Fatal(ctx, "Failed to deserialize tx : %s", err)
	}

	if err := spyNode.SendTx(ctx, tx); err != nil {
		logger.Fatal(ctx, "Failed to send tx : %s", err)
	}

	logger.Info(ctx, "Sent tx : %s", tx.TxHash())
}

func Subscribe(ctx context.Context, spyNode *client.RemoteClient, args []string) {
	if len(args) != 1 {
		logger.Fatal(ctx, "Wrong argument count: subscribe [Bitcoin Address]")
	}

	ad, err := bitcoin.DecodeAddress(args[0])
	if err != nil {
		logger.Fatal(ctx, "Failed to parse address : %s", err)
	}
	ra := bitcoin.NewRawAddressFromAddress(ad)

	if err := client.SubscribeAddresses(ctx, []bitcoin.RawAddress{ra}, spyNode); err != nil {
		logger.Fatal(ctx, "Failed to subscribe to address : %s", err)
	}

	logger.Info(ctx, "Subscribed to address : %s", ad)
	time.Sleep(time.Second) // TODO Build wait that waits for spynode client to complete
}

func MarkHeaderInvalid(ctx context.Context, spyNode *client.RemoteClient, args []string) {
	if len(args) != 1 {
		logger.Fatal(ctx, "Wrong argument count: mark_header_invalid [Block Hash]")
	}

	blockHash, err := bitcoin.NewHash32FromStr(args[0])
	if err != nil {
		logger.Fatal(ctx, "Failed to decode block hash : %s", err)
	}

	if err := spyNode.MarkHeaderInvalid(ctx, *blockHash); err != nil {
		logger.Fatal(ctx, "Failed to mark header invalid : %s", err)
	}

	logger.Info(ctx, "Marked header invalid : %s", blockHash)
}

func MarkHeaderNotInvalid(ctx context.Context, spyNode *client.RemoteClient, args []string) {
	if len(args) != 1 {
		logger.Fatal(ctx, "Wrong argument count: mark_header_not_invalid [Block Hash]")
	}

	blockHash, err := bitcoin.NewHash32FromStr(args[0])
	if err != nil {
		logger.Fatal(ctx, "Failed to decode block hash : %s", err)
	}

	if err := spyNode.MarkHeaderNotInvalid(ctx, *blockHash); err != nil {
		logger.Fatal(ctx, "Failed to mark header not invalid : %s", err)
	}

	logger.Info(ctx, "Marked header not invalid : %s", blockHash)
}

type Handler struct {
	SpyNode *client.RemoteClient

	ready     chan interface{}
	readyLock sync.Mutex

	lastMessageID uint64
}

func NewHandler(spyNode *client.RemoteClient) *Handler {
	return &Handler{
		SpyNode: spyNode,
		ready:   make(chan interface{}),
	}
}

func (h *Handler) GetReadyChannel() <-chan interface{} {
	return h.ready
}

func (h *Handler) HandleTx(ctx context.Context, tx *client.Tx) {
	h.lastMessageID = tx.ID
	js, _ := json.MarshalIndent(tx.State, "  ", "  ")
	fmt.Printf("Received tx : \n  %s\n%s\n", string(js), tx.Tx)

	fmt.Printf("  Spent Outputs : \n")
	for _, output := range tx.Outputs {
		fmt.Printf("    Value : %d\n", output.Value)
		fmt.Printf("    Script : %s\n", output.LockingScript)
	}
}

func (h *Handler) HandleTxUpdate(ctx context.Context, update *client.TxUpdate) {
	h.lastMessageID = update.ID
	js, _ := json.MarshalIndent(update, "  ", "  ")
	fmt.Printf("Received tx update : \n  %s\n", string(js))
}

func (h *Handler) HandleHeaders(ctx context.Context, headers *client.Headers) {
	js, _ := json.MarshalIndent(headers, "  ", "  ")
	fmt.Printf("Received headers : \n  %s\n", string(js))
}

func (h *Handler) HandleInSync(ctx context.Context) {
	fmt.Printf("Received In Sync\n")
}

// HandleMessage handles all other client data messages
func (h *Handler) HandleMessage(ctx context.Context, payload client.MessagePayload) {
	switch msg := payload.(type) {
	case *client.AcceptRegister:
		js, _ := json.MarshalIndent(msg, "  ", "  ")
		fmt.Printf("Received accept message : \n  %s\n", string(js))

		if err := h.SpyNode.Ready(ctx, h.lastMessageID+1); err != nil {
			logger.Error(ctx, "Failed to send ready : %s", err)
			return
		}
		h.readyLock.Lock()
		if h.ready != nil {
			close(h.ready)
			h.ready = nil
		}
		h.readyLock.Unlock()

	default:
		js, _ := json.MarshalIndent(msg, "  ", "  ")
		fmt.Printf("Received other message : \n  %s\n", string(js))
	}
}
