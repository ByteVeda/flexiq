package door

// FromRPC converts what a gRPC call returned into the root package's error.
//
// The root package sets it in its init, because the error type is the root's
// and this package cannot import it back. A caller of this hook imports the
// root package too, so Go has run that init before any call reaches here.
var FromRPC func(error) error
