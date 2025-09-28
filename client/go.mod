module echo-client

go 1.21

require (
	github.com/streadway/amqp v1.1.0
	shared/rabbitmq v0.0.0
)

replace shared/rabbitmq => ../shared/rabbitmq
