package util

type EventType string

const (
	//bucket
	EVENTTYPE_ADD_BUCKET    EventType = "add-bucket"
	EVENTTYPE_DELETE_BUCKET EventType = "delete-bucket"

	//object
	EVENTTYPE_ADD_OBJECT    EventType = "add-object"
	EVENTTYPE_DELETE_OBJECT EventType = "delete-object"

	EVENTTYPE_OBJECT  EventType = "object"
	EVENTTYPE_LOGGING EventType = "logging"
)
