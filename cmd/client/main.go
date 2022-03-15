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
	"syscall"
	"time"

	"github.com/tokenized/config"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
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
	defer spyNode.Close(ctx)

	if len(os.Args) < 2 {
		logger.Fatal(ctx, "Not enough arguments. Need command (listen, send_tx, subscribe)")
	}

	if os.Args[1] == "listen" {
		cfg.ConnectionType = client.ConnectionTypeFull
	} else {
		cfg.ConnectionType = client.ConnectionTypeControl
	}

	switch os.Args[1] {
	case "listen":
		Listen(ctx, spyNode, os.Args[2:])

	case "send_tx":
		SendTx(ctx, spyNode, os.Args[2:])

	case "subscribe":
		Subscribe(ctx, spyNode, os.Args[2:])
	}
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

func Listen(ctx context.Context, spyNode *client.RemoteClient, args []string) {
	handler := NewHandler(spyNode)

	spyNode.RegisterHandler(handler)

	spynodeErrors := make(chan error, 1)
	spyNode.SetListenerErrorChannel(&spynodeErrors)

	if err := spyNode.Connect(ctx); err != nil {
		logger.Error(ctx, "Failed to connect : %s", err)
		return
	}

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-spynodeErrors:
		if err != nil {
			logger.Error(ctx, "Spynode returned errors")
		}

	case <-handler.Done:
		spyNode.Close(ctx)
		logger.Info(ctx, "Handler finished")

		select {
		case err := <-spynodeErrors:
			if err != nil {
				logger.Error(ctx, "Spynode returned errors : %s", err)
			}
		}

	case <-osSignals:
		logger.Info(ctx, "Shutdown requested")
		spyNode.Close(ctx)

		select {
		case err := <-spynodeErrors:
			if err != nil {
				logger.Error(ctx, "Spynode returned errors")
			}
		}
	}
}

type Handler struct {
	SpyNode *client.RemoteClient

	Done chan interface{}

	lastMessageID uint64
}

func NewHandler(spyNode *client.RemoteClient) *Handler {
	return &Handler{
		SpyNode: spyNode,
		Done:    make(chan interface{}),
	}
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
	default:
		js, _ := json.MarshalIndent(msg, "  ", "  ")
		fmt.Printf("Received other message : \n  %s\n", string(js))
	}
}
