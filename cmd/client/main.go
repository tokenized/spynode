package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/tokenized/config"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/spynode/pkg/client"
)

var (
	buildVersion = "unknown"
	buildDate    = "unknown"
	buildUser    = "unknown"
)

func main() {
	// -------------------------------------------------------------------------
	// Logging

	logPath := os.Getenv("LOG_FILE_PATH")

	logConfig := logger.NewConfig(strings.ToUpper(os.Getenv("DEVELOPMENT")) == "TRUE",
		strings.ToUpper(os.Getenv("LOG_FORMAT")) == "TEXT", logPath)

	ctx := logger.ContextWithLogConfig(context.Background(), logConfig)

	// -------------------------------------------------------------------------
	// Config

	cfg := Config{}

	// load config using sane fallbacks
	if err := config.LoadConfig(ctx, &cfg); err != nil {
		logger.Fatal(ctx, "main : LoadConfig : %v", err)
	}

	config.DumpSafe(ctx, &cfg)

	keyStr := os.Getenv("CLIENT_WALLET_KEY")
	key, err := bitcoin.KeyFromStr(keyStr)
	if err != nil {
		logger.Fatal(ctx, "Failed to parse key : %s", err)
	}

	address, err := key.RawAddress()
	if err != nil {
		logger.Fatal(ctx, "Failed to generate address : %s", err)
	}

	lockingScript, err := address.LockingScript()
	if err != nil {
		logger.Fatal(ctx, "Failed to generate locking script : %s", err)
	}

	// -------------------------------------------------------------------------
	// Create tx

	tx := txbuilder.NewTxBuilder(0.5, 0.25)

	hash, err := bitcoin.NewHash32FromStr("c04448bee2b6993505a7be44c38ed0846cb27c44620c546202fefffff3153248")
	if err != nil {
		logger.Fatal(ctx, "Failed to parse txid : %s", err)
	}

	utxo := bitcoin.UTXO{
		Hash:          *hash,
		Index:         0,
		Value:         2977511,
		LockingScript: lockingScript,
	}

	if err := tx.AddInputUTXO(utxo); err != nil {
		logger.Fatal(ctx, "Failed to add input : %s", err)
	}

	if err := tx.SetChangeAddress(address, ""); err != nil {
		logger.Fatal(ctx, "Failed to set change address : %s", err)
	}

	if err := tx.Sign([]bitcoin.Key{key}); err != nil {
		logger.Fatal(ctx, "Failed to sign tx : %s", err)
	}

	fmt.Printf(tx.String(cfg.Net) + "\n")

	// -------------------------------------------------------------------------
	// Spynode

	spyNodeConfig, err := client.ConvertEnvConfig(&cfg.SpyNode)
	if err != nil {
		logger.Fatal(ctx, "main: Convert spynode config : %s", err)
	}

	spyNodeConfig.ConnectionType = client.ConnectionTypeControl

	spyNode, err := client.NewRemoteClient(spyNodeConfig)
	if err != nil {
		logger.Fatal(ctx, "Failed to create spynode client : %s", err)
	}
	defer spyNode.Close(ctx)

	if err := client.SubscribeAddresses(ctx, []bitcoin.RawAddress{address}, spyNode); err != nil {
		logger.Fatal(ctx, "Failed to subscribe to address : %s", err)
	}

	if err := spyNode.SendTx(ctx, tx.MsgTx); err != nil {
		logger.Fatal(ctx, "Failed to send tx : %s", err)
	}
}

type Config struct {
	Net     bitcoin.Network
	SpyNode client.EnvConfig
}
