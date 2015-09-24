package main

import (
	"github.com/lixin9311/lfshook"
	"github.com/lixin9311/logrus"
	"time"
)

var log = logrus.New()

func init() {
	log.Formatter = new(logrus.JSONFormatter)
	tmp := new(logrus.TextFormatter) // default
	tmp.ForceUnColored = false
	tmp.DisableTimestamp = true
	log.Formatter = tmp
	log.Level = logrus.DebugLevel
	tmp2 := new(logrus.TextFormatter) // default
	tmp2.ForceUnColored = true
	tmp2.DisableTimestamp = true
	log.Hooks.Add(lfshook.NewHook(lfshook.PathMap{
		logrus.InfoLevel:  "log.log",
		logrus.ErrorLevel: "log.log",
		logrus.DebugLevel: "log.log",
		logrus.PanicLevel: "log.log",
		logrus.FatalLevel: "log.log",
		logrus.WarnLevel:  "log.log",
	}, new(logrus.JSONFormatter)))
}

func main() {
	defer func() {
		err := recover()
		if err != nil {
			log.WithFields(logrus.Fields{
				"omg":    true,
				"err":    err,
				"number": 100,
			}).Fatal("The ice breaks!")
		}
	}()

	log.WithFields(logrus.Fields{
		"animal": "walrus",
		"number": 8,
	}).Debug("Started observing beach")
	time.Sleep(time.Second)
	log.WithFields(logrus.Fields{
		"animal": "walrus",
		"size":   10,
	}).Info("A group of walrus emerges from the ocean")

	log.WithFields(logrus.Fields{
		"omg":    true,
		"number": 122,
	}).Warn("The group's number increased tremendously!")

	log.WithFields(logrus.Fields{
		"temperature": -4,
	}).Debug("Temperature changes")

	log.WithFields(logrus.Fields{
		"animal": "orca",
		"size":   9009,
	}).Panic("It's over 9000!")
}
