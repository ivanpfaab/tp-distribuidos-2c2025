module echo-client

go 1.21

require batch v0.0.0

require common v0.0.0 // indirect

replace batch => ../protocol/batch

replace common => ../protocol/common