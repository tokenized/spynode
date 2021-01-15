package client

import (
	"github.com/tokenized/pkg/bitcoin"
)

type Config struct {
	ServerAddress    string            `envconfig:"SERVER_ADDRESS" json:"SERVER_ADDRESS"`
	ServerKey        bitcoin.PublicKey `envconfig:"SERVER_KEY" json:"SERVER_KEY"`
	ClientKey        bitcoin.Key       `envconfig:"CLIENT_KEY" json:"CLIENT_KEY" masked:"true"`
	StartBlockHeight uint32            `default:"478559" envconfig:"START_BLOCK_HEIGHT" json:"START_BLOCK_HEIGHT"`
}

func NewConfig(serverAddress string, serverKey bitcoin.PublicKey, clientKey bitcoin.Key,
	startBlockHeight uint32) *Config {
	return &Config{
		ServerAddress:    serverAddress,
		ServerKey:        serverKey,
		ClientKey:        clientKey,
		StartBlockHeight: startBlockHeight,
	}
}
