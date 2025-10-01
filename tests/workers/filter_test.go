package workers

import (
	"testing"
	"time"

	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
	"github.com/tp-distribuidos-2c2025/workers/filter"

	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

func TestWorkerFilter(t *testing.T) {

	testing_utils.InitLogger()
	testing_utils.LogTest("Testing Exchange One-to-One pattern")

	// Init connection
	config := &middleware.ConnectionConfig{
		URL: "amqp://admin:password@rabbitmq:5672/",
	}
	err := middleware.WaitForConnection(config, 10, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	testing_utils.LogStep("Connected to RabbitMQ")

	// Init middleware
	testing_utils.LogStep("Creating exchange producer and consumer")
	producer := exchange.NewMessageMiddlewareExchange("filter-exchange", []string{"test.key"}, config)
	filterWorker, err := filter.NewFilterWorker(config)

	if producer == nil || filterWorker == nil {
		t.Fatal("Failed to create middleware")
	}

	// Declare exchange
	testing_utils.LogStep("Declaring exchange")
	errCode := producer.DeclareExchange("topic", true, false, false, false)
	if errCode != 0 {
		t.Fatalf("Failed to declare exchange: %v", errCode)
	}

}
