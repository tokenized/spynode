package client

import (
	"time"

	"github.com/tokenized/config"
	"github.com/tokenized/pkg/bitcoin"
)

// EnvConfig has environment safe types for importing from environment values.
// Apparently it doesn't use the TextMarshaler interfaces.
type Config struct {
	ServerAddress    string            `envconfig:"SPYNODE_SERVER_ADDRESS" json:"server_address"`
	ServerKey        bitcoin.PublicKey `envconfig:"SPYNODE_SERVER_KEY" json:"server_key"`
	ClientKey        bitcoin.Key       `envconfig:"SPYNODE_CLIENT_KEY" json:"client_key" masked:"true"`
	StartBlockHeight uint32            `default:"743000" envconfig:"SPYNODE_START_BLOCK_HEIGHT" json:"start_block_height"`
	ConnectionType   ConnectionType    `default:"full" envconfig:"SPYNODE_CONNECTION_TYPE" json:"connection_type"`

	MaxRetries int             `default:"50" envconfig:"SPYNODE_MAX_RETRIES" json:"max_retries"`
	RetryDelay config.Duration `default:"2s" envconfig:"SPYNODE_RETRY_DELAY" json:"retry_delay"`
	RetryError config.Duration `default:"10m" envconfig:"SPYNODE_RETRY_ERROR" json:"retry_error"`

	RequestTimeout config.Duration `default:"10s" envconfig:"SPYNODE_REQUEST_TIMEOUT" json:"request_timeout"`
	DialTimeout    config.Duration `default:"5s" envconfig:"SPYNODE_DIAL_TIMEOUT" json:"dial_timeout"`

	HandshakeTimeout      config.Duration `default:"30s" envconfig:"HANDSHAKE_TIMEOUT" json:"handshake_timeout"`
	MessageChannelTimeout config.Duration `default:"30s" envconfig:"MESSAGE_CHANNEL_TIMEOUT" json:"message_channel_timeout"`
}

func NewConfig(serverAddress string, serverKey bitcoin.PublicKey, clientKey bitcoin.Key,
	startBlockHeight uint32, connectionType ConnectionType) *Config {
	return &Config{
		ServerAddress:         serverAddress,
		ServerKey:             serverKey,
		ClientKey:             clientKey,
		StartBlockHeight:      startBlockHeight,
		ConnectionType:        connectionType,
		MaxRetries:            50,
		RetryDelay:            config.NewDuration(time.Second * 2),
		RequestTimeout:        config.NewDuration(time.Second * 10),
		DialTimeout:           config.NewDuration(time.Second * 5),
		HandshakeTimeout:      config.NewDuration(time.Second * 30),
		MessageChannelTimeout: config.NewDuration(time.Second * 30),
	}
}
