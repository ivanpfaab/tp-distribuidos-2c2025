module echo-server

go 1.21

require (
	common v0.0.0
	github.com/rabbitmq/amqp091-go v1.9.0
)

replace common => ./protocol/common
