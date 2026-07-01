package chainstore

// NodeIDFor exposes the internal nodeID derivation for use in tests that need
// to inspect backend state (e.g. calling backend.GetNode to verify fields).
var NodeIDFor = nodeID
