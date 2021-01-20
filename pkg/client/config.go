package client

import (
	"github.com/tokenized/pkg/bitcoin"
)

type Config struct {
	ServerAddress    string            `envconfig:"SPYNODE_SERVER_ADDRESS" json:"SPYNODE_SERVER_ADDRESS"`
	ServerKey        bitcoin.PublicKey `envconfig:"SPYNODE_SERVER_KEY" json:"SPYNODE_SERVER_KEY"`
	ClientKey        bitcoin.Key       `envconfig:"SPYNODE_CLIENT_KEY" json:"SPYNODE_CLIENT_KEY" masked:"true"`
	StartBlockHeight uint32            `default:"478559" envconfig:"SPYNODE_START_BLOCK_HEIGHT" json:"SPYNODE_START_BLOCK_HEIGHT"`
	ConnectionType   uint8             `default:"1" envconfig:"SPYNODE_CONNECTION_TYPE" json:"SPYNODE_CONNECTION_TYPE"`

	MaxRetries int `default:"20" envconfig:"SPYNODE_MAX_RETRIES"`
	RetryDelay int `default:"2000" envconfig:"SPYNODE_RETRY_DELAY"`
}

func NewConfig(serverAddress string, serverKey bitcoin.PublicKey, clientKey bitcoin.Key,
	startBlockHeight uint32, connectionType uint8) *Config {
	return &Config{
		ServerAddress:    serverAddress,
		ServerKey:        serverKey,
		ClientKey:        clientKey,
		StartBlockHeight: startBlockHeight,
		ConnectionType:   connectionType,
		MaxRetries:       20,
		RetryDelay:       2000,
	}
}
