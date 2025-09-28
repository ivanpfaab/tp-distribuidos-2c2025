module tp-distribuidos-2c2025/workers/group_by

go 1.21

require (
	tp-distribuidos-2c2025/protocol/chunk v0.0.0
	tp-distribuidos-2c2025/shared/middleware v0.0.0
	tp-distribuidos-2c2025/shared/middleware/exchange v0.0.0
)

require github.com/rabbitmq/amqp091-go v1.9.0 // indirect

replace tp-distribuidos-2c2025/protocol/chunk => ../../protocol/chunk
replace tp-distribuidos-2c2025/shared/middleware => ../../shared/middleware
replace tp-distribuidos-2c2025/shared/middleware/exchange => ../../shared/middleware/exchange
