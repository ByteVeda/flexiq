package flexiq

// Version is the FlexiQ release this client is published alongside. It rides
// on every call as the gRPC user agent, so a server log names the client build
// without the client having to say so.
//
// Kept in step with the workspace version by scripts/version.mjs. Never edit
// it by hand.
const Version = "2.0.0"
