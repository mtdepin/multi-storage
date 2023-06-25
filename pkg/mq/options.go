package mq

type sendOptions struct {
	Track         bool
	TrackId       TrackID       // track id
	TrackCallback TrackCallback // track callback
	topic         string
	tag           string
}

type SendOption func(o *sendOptions)

func newSendOptions(opts ...SendOption) sendOptions {
	opt := sendOptions{}
	for _, o := range opts {
		o(&opt)
	}
	return opt
}

func WithTrackID(tid TrackID) SendOption {
	return func(o *sendOptions) {
		o.TrackId = tid
	}
}

func WithTrackCallback(cb TrackCallback) SendOption {
	return func(o *sendOptions) {
		o.TrackCallback = cb
	}
}

func WithTrack() SendOption {
	return func(o *sendOptions) {
		o.Track = true
	}
}

func WithTopic(topic string) SendOption {
	return func(o *sendOptions) {
		o.topic = topic
	}
}

func WithTag(tag string) SendOption {
	return func(o *sendOptions) {
		o.topic = tag
	}
}
