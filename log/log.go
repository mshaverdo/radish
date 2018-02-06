package log

import (
	"github.com/op/go-logging"
	"os"
)

const moduleName = "main"

const (
	CRITICAL = logging.CRITICAL
	ERROR    = logging.ERROR
	WARNING  = logging.WARNING
	NOTICE   = logging.NOTICE
	INFO     = logging.INFO
	DEBUG    = logging.DEBUG
)

var logger = logging.MustGetLogger(moduleName)
var format = logging.MustStringFormatter(
	`%{color}%{time:15:04:05.000} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`,
)

func init() {
	backend := logging.NewLogBackend(os.Stderr, "", 0)

	// For messages written to backend we want to add some additional
	// information to the output, including the used log level and the name of
	// the function.
	backendFormatter := logging.NewBackendFormatter(backend, format)

	// Set the backend to be used.
	logging.SetBackend(backendFormatter)
}

// SetLevel sets current global log level for the logger
func SetLevel(level logging.Level) {
	logging.SetLevel(level, moduleName)
}

// Criticalf logs a message using CRITICAL as log level.
func Criticalf(format string, args ...interface{}) {
	logger.Critical(format, args...)
}

// Critical logs a message using CRITICAL as log level.
func Critical(format string, args ...interface{}) {
	logger.Critical(format, args...)
}

// Errorf logs a message using ERROR as log level.
func Errorf(format string, args ...interface{}) {
	logger.Error(format, args...)
}

// Error logs a message using ERROR as log level.
func Error(format string, args ...interface{}) {
	logger.Error(format, args...)
}

// Warningf logs a message using WARNING as log level.
func Warningf(format string, args ...interface{}) {
	logger.Warningf(format, args...)
}

// Warning logs a message using WARNING as log level.
func Warning(format string, args ...interface{}) {
	logger.Warning(format, args...)
}

// Noticef logs a message using NOTICE as log level.
func Noticef(format string, args ...interface{}) {
	logger.Noticef(format, args...)
}

// Notice logs a message using NOTICE as log level.
func Notice(format string, args ...interface{}) {
	logger.Notice(format, args...)
}

// Infof logs a message using INFO as log level.
func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

// Info logs a message using INFO as log level.
func Info(format string, args ...interface{}) {
	logger.Info(format, args...)
}

// Debugf logs a message using DEBUG as log level.
func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

// Debug logs a message using DEBUG as log level.
func Debug(format string, args ...interface{}) {
	logger.Debug(format, args...)
}
