package config

import "net"

// deriveCharonURL returns an HTTP URL for the Charon internal API from its
// listen address. Wildcard hosts (empty, "0.0.0.0", "::") are replaced with
// "127.0.0.1" so the proxy connects to localhost.
func deriveCharonURL(charonListen string) string {
	host, port, err := net.SplitHostPort(charonListen)
	if err != nil {
		return "http://127.0.0.1:8081"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}
