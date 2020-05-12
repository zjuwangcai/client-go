package tikv

import "github.com/pkg/errors"

var(
	errNoMoreRequiredDataFromTikv = errors.New("no more data from tikv")
	errNilClient                  = errors.New("client is nil")
)
