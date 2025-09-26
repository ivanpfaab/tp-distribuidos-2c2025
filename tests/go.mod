module tp-distribuidos-2c2025/tests

go 1.21

require (
	github.com/streadway/amqp v1.1.0
	github.com/stretchr/testify v1.8.4
	tp-distribuidos-2c2025/protocol/batch v0.0.0-00010101000000-000000000000
	tp-distribuidos-2c2025/protocol/chunk v0.0.0-00010101000000-000000000000
	tp-distribuidos-2c2025/shared/rabbitmq v0.0.0-00010101000000-000000000000
)

replace tp-distribuidos-2c2025/protocol/batch => ../protocol/batch

replace tp-distribuidos-2c2025/protocol/chunk => ../protocol/chunk

replace tp-distribuidos-2c2025/shared/rabbitmq => ../shared/rabbitmq

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
