package asq

import "github.com/golang/glog"

type Logger interface {
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

type defaultLogger struct{}

func (dl defaultLogger) Infof(format string, args ...interface{}) {
	glog.Infof(format, args...)
}

func (dl defaultLogger) Errorf(format string, args ...interface{}) {
	glog.Errorf(format, args...)
}

func (dl defaultLogger) Fatalf(format string, args ...interface{}) {
	glog.Fatalf(format, args...)
}
