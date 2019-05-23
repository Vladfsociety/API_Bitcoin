package main

import(
   //"fmt"
   "github.com/pkg/errors"
 )

type customError struct {
  originalError error
}

func (error customError) Error() string {
   return error.originalError.Error()
}

func NewErr(msg string) error {
   return customError{originalError: errors.New(msg)}
}

func Wrap(err error, msg string) error {
   return Wrapf(err, msg)
}

func Wrapf(err error, msg string, args ...interface{}) error {
   return customError{originalError: errors.Wrapf(err, msg, args...)}
}
