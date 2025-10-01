package producers

import (
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/exchange"
)

// Producers manages all exchange producers for the query orchestrator
type Producers struct {
	FilterProducer     *exchange.ExchangeMiddleware
	AggregatorProducer *exchange.ExchangeMiddleware
	JoinProducer       *exchange.ExchangeMiddleware
	GroupByProducer    *exchange.ExchangeMiddleware
	StreamingProducer  *exchange.ExchangeMiddleware
}

// NewProducers creates a new Producers instance
func NewProducers() *Producers {
	return &Producers{}
}

// Initialize initializes all exchange producers
func (p *Producers) Initialize(config *middleware.ConnectionConfig) middleware.MessageMiddlewareError {
	// Initialize Filter producer
	p.FilterProducer = exchange.NewMessageMiddlewareExchange(
		"filter-exchange",
		[]string{"filter"},
		config,
	)
	if p.FilterProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Initialize Aggregator producer
	p.AggregatorProducer = exchange.NewMessageMiddlewareExchange(
		"aggregator-exchange",
		[]string{"aggregator"},
		config,
	)
	if p.AggregatorProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Initialize Join producer
	p.JoinProducer = exchange.NewMessageMiddlewareExchange(
		"join-exchange",
		[]string{"join"},
		config,
	)
	if p.JoinProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Initialize Group By producer
	p.GroupByProducer = exchange.NewMessageMiddlewareExchange(
		"groupby-exchange",
		[]string{"groupby"},
		config,
	)
	if p.GroupByProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	// Initialize Streaming producer
	p.StreamingProducer = exchange.NewMessageMiddlewareExchange(
		"streaming-exchange",
		[]string{"streaming"},
		config,
	)
	if p.StreamingProducer == nil {
		return middleware.MessageMiddlewareDisconnectedError
	}

	return 0
}

// DeclareExchanges declares all exchanges on RabbitMQ
func (p *Producers) DeclareExchanges() middleware.MessageMiddlewareError {
	// Declare Filter exchange
	if err := p.FilterProducer.DeclareExchange("topic", true, false, false, false); err != 0 {
		return err
	}

	// Declare Aggregator exchange
	if err := p.AggregatorProducer.DeclareExchange("topic", true, false, false, false); err != 0 {
		return err
	}

	// Declare Join exchange
	if err := p.JoinProducer.DeclareExchange("topic", true, false, false, false); err != 0 {
		return err
	}

	// Declare Group By exchange
	if err := p.GroupByProducer.DeclareExchange("topic", true, false, false, false); err != 0 {
		return err
	}

	// Declare Streaming exchange
	if err := p.StreamingProducer.DeclareExchange("topic", true, false, false, false); err != 0 {
		return err
	}

	return 0
}

// Close closes all producer connections
func (p *Producers) Close() middleware.MessageMiddlewareError {
	var lastErr middleware.MessageMiddlewareError

	if p.FilterProducer != nil {
		if err := p.FilterProducer.Close(); err != 0 {
			lastErr = err
		}
	}

	if p.AggregatorProducer != nil {
		if err := p.AggregatorProducer.Close(); err != 0 {
			lastErr = err
		}
	}

	if p.JoinProducer != nil {
		if err := p.JoinProducer.Close(); err != 0 {
			lastErr = err
		}
	}

	if p.GroupByProducer != nil {
		if err := p.GroupByProducer.Close(); err != 0 {
			lastErr = err
		}
	}

	if p.StreamingProducer != nil {
		if err := p.StreamingProducer.Close(); err != 0 {
			lastErr = err
		}
	}

	return lastErr
}

// SendToFilter sends a message to the filter exchange
func (p *Producers) SendToFilter(data []byte) middleware.MessageMiddlewareError {
	return p.FilterProducer.Send(data, []string{"filter"})
}

// SendToAggregator sends a message to the aggregator exchange
func (p *Producers) SendToAggregator(data []byte) middleware.MessageMiddlewareError {
	return p.AggregatorProducer.Send(data, []string{"aggregator"})
}

// SendToJoin sends a message to the join exchange
func (p *Producers) SendToJoin(data []byte) middleware.MessageMiddlewareError {
	return p.JoinProducer.Send(data, []string{"join"})
}

// SendToGroupBy sends a message to the groupby exchange
func (p *Producers) SendToGroupBy(data []byte) middleware.MessageMiddlewareError {
	return p.GroupByProducer.Send(data, []string{"groupby"})
}

// SendToStreaming sends a message to the streaming exchange
func (p *Producers) SendToStreaming(data []byte) middleware.MessageMiddlewareError {
	return p.StreamingProducer.Send(data, []string{"streaming"})
}
