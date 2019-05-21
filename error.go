package main

import "github.com/pkg/errors"

const(
  NoType = ErrorType(iota)
  NotFound
  //add any type you want
)
type ErrorType uint

type customError struct {
  errorType ErrorType
  originalError error
}

func (error customError) Error() string {
   return error.originalError.Error()
}

func (typer ErrorType) Wrap(err error, msg string) error {
   return typer.Wrapf(err, msg)
}

func (typer ErrorType) Wrapf(err error, msg string, args ...interface{}) error {
   newErr := errors.Wrapf(err, msg, args...)
   return customError{errorType: typer, originalError: newErr}
}

func Wrap(err error, msg string) error {
   return Wrapf(err, msg)
}

func Wrapf(err error, msg string, args ...interface{}) error {
   wrappedError := errors.Wrapf(err, msg, args...)
   if customErr, ok := err.(customError); ok {
      return customError{
         errorType: customErr.errorType,
         originalError: wrappedError,
      }
   }
   return customError{errorType: NoType, originalError: wrappedError}
}

func GetType(err error) ErrorType {
   if customErr, ok := err.(customError); ok {
      return customErr.errorType
   }
   return NoType
}
