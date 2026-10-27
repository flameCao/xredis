package xlog

import "testing"

func TestLog(t *testing.T) {
	Info("info")
	Debug("debug")
	Warn("warn")
	Error("error")
	Color(Green).Info("color info")
	Color(Cyan).Debug("color debug")
	Color(Yellow).Warn("color warn")
	Color(Red).Error("color error")
}
