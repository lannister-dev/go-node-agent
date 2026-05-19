package badger

import (
	"fmt"
	"log/slog"
)

type badgerSlogAdapter struct {
	log *slog.Logger
}

func (a badgerSlogAdapter) Errorf(format string, args ...any) {
	a.log.Error(fmt.Sprintf(format, args...))
}

func (a badgerSlogAdapter) Warningf(format string, args ...any) {
	a.log.Warn(fmt.Sprintf(format, args...))
}

func (a badgerSlogAdapter) Infof(format string, args ...any) {
	a.log.Info(fmt.Sprintf(format, args...))
}

func (a badgerSlogAdapter) Debugf(format string, args ...any) {
	a.log.Debug(fmt.Sprintf(format, args...))
}
