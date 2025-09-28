module echo-server

go 1.21

require (
	github.com/rabbitmq/amqp091-go v1.10.0
	tp-distribuidos-2c2025/protocol/deserializer v0.0.0
)

require (
	tp-distribuidos-2c2025/protocol/batch v0.0.0 // indirect
	tp-distribuidos-2c2025/protocol/chunk v0.0.0 // indirect
	tp-distribuidos-2c2025/protocol/common v0.0.0 // indirect
)

replace tp-distribuidos-2c2025/protocol/batch => ../protocol/batch

replace tp-distribuidos-2c2025/protocol/chunk => ../protocol/chunk

replace tp-distribuidos-2c2025/protocol/common => ../protocol/common

replace tp-distribuidos-2c2025/protocol/deserializer => ../protocol/deserializer
