package sqlt

type Error struct {
	err error
}

func (e Error) Error() string {
	return e.err.Error()
}

func (e Error) Unwrap() error {
	return e.err
}

func (e Error) Is(target error) bool {
	_, ok := target.(Error)
	return ok
}

func (e Error) As(target any) bool {
	if t, ok := target.(*Error); ok {
		*t = e
		return true
	}
	return false
}
