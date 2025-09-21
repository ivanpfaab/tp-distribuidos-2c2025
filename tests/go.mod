module tp-distribuidos-2c2025/tests

go 1.21

require (
	github.com/stretchr/testify v1.8.4
)

replace tp-distribuidos-2c2025/protocol/batch => ../protocol/batch
replace tp-distribuidos-2c2025/protocol/chunk => ../protocol/chunk

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
