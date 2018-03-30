package util

import (
	"flag"

	log "github.com/sirupsen/logrus"
)

var (
	level = flag.Int("log-level", 2, "Set the log level of the application (1=DEBUG|2=INFO|3=WARN|4=ERROR|5=FATAL|6=PANIC)")
)

func InitLogger() {
	switch *level {
	case 1:
		log.SetLevel(log.DebugLevel)
	case 2:
		log.SetLevel(log.InfoLevel)
	case 3:
		log.SetLevel(log.WarnLevel)
	case 4:
		log.SetLevel(log.ErrorLevel)
	case 5:
		log.SetLevel(log.FatalLevel)
	case 6:
		log.SetLevel(log.PanicLevel)
	}
	log.Infof("log level set to %v", log.GetLevel())
}
