package election

type ElectionEventType byte

const (
	ElectionStart  ElectionEventType = 0x01
	ElectionOk     ElectionEventType = 0x02
	ElectionLeader ElectionEventType = 0x03
)

const (
	SupervisorIDSize = 4
	TimestampSize    = 8
	EventTypeSize    = 1
)
