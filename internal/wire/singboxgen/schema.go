package singboxgen

type singBoxConfig struct {
	Log          *logConfig    `json:"log,omitempty"`
	Inbounds     []inbound     `json:"inbounds"`
	Outbounds    []outbound    `json:"outbounds"`
	Route        routeConfig   `json:"route"`
	Experimental *experimental `json:"experimental,omitempty"`
}

type logConfig struct {
	Level    string `json:"level,omitempty"`
	Disabled bool   `json:"disabled,omitempty"`
}

type experimental struct {
	ClashAPI clashAPIConfig `json:"clash_api"`
}

type clashAPIConfig struct {
	ExternalController string `json:"external_controller"`
	Secret             string `json:"secret,omitempty"`
}

type inbound struct {
	Type       string      `json:"type"`
	Tag        string      `json:"tag"`
	Listen     string      `json:"listen,omitempty"`
	ListenPort uint16      `json:"listen_port"`
	Sniff      bool        `json:"sniff,omitempty"`
	Users      []vlessUser `json:"users"`
	TLS        *tlsConfig  `json:"tls,omitempty"`
}

type vlessUser struct {
	Name string `json:"name"`
	UUID string `json:"uuid"`
	Flow string `json:"flow,omitempty"`
}

type outbound struct {
	Type                      string     `json:"type"`
	Tag                       string     `json:"tag"`
	Server                    string     `json:"server,omitempty"`
	ServerPort                uint16     `json:"server_port,omitempty"`
	UUID                      string     `json:"uuid,omitempty"`
	Flow                      string     `json:"flow,omitempty"`
	TLS                       *tlsConfig `json:"tls,omitempty"`
	Outbounds                 []string   `json:"outbounds,omitempty"`
	Default                   string     `json:"default,omitempty"`
	InterruptExistConnections bool       `json:"interrupt_exist_connections,omitempty"`
}

type tlsConfig struct {
	Enabled    bool           `json:"enabled"`
	ServerName string         `json:"server_name,omitempty"`
	Reality    *realityConfig `json:"reality,omitempty"`
}

type realityConfig struct {
	Enabled    bool      `json:"enabled"`
	Handshake  handshake `json:"handshake"`
	PrivateKey string    `json:"private_key,omitempty"`
	ShortID    []string  `json:"short_id,omitempty"`
}

type handshake struct {
	Server     string `json:"server"`
	ServerPort uint16 `json:"server_port"`
}

type routeConfig struct {
	Rules               []routeRule `json:"rules"`
	AutoDetectInterface bool        `json:"auto_detect_interface"`
	Final               string      `json:"final,omitempty"`
}

type routeRule struct {
	AuthUser []string `json:"auth_user,omitempty"`
	Outbound string   `json:"outbound"`
}
