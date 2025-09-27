module tp-distribuidos-2c2025/tests

go 1.21

require (
	github.com/rabbitmq/amqp091-go v1.9.0
	github.com/stretchr/testify v1.8.4
	tp-distribuidos-2c2025/protocol/batch v0.0.0
	tp-distribuidos-2c2025/protocol/chunk v0.0.0
	tp-distribuidos-2c2025/shared/middleware v0.0.0
	tp-distribuidos-2c2025/shared/middleware/exchange v0.0.0
	tp-distribuidos-2c2025/shared/middleware/workerqueue v0.0.0
)

replace tp-distribuidos-2c2025/protocol/batch => ./protocol/batch
replace tp-distribuidos-2c2025/protocol/chunk => ./protocol/chunk
replace tp-distribuidos-2c2025/shared/middleware => ./shared/middleware
replace tp-distribuidos-2c2025/shared/middleware/exchange => ./shared/middleware/exchange
replace tp-distribuidos-2c2025/shared/middleware/workerqueue => ./shared/middleware/workerqueue
