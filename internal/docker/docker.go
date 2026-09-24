package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

const sockPath = "/var/run/docker.sock"

type NodeContainer struct {
	ContainerID string `json:"container"`
	Name        string `json:"name"`
	NodeID      string `json:"node_id"`
	State       string `json:"state"`
	Image       string `json:"image"`
	HostPort    string `json:"host_port,omitempty"`
}

type Client struct {
	http   *http.Client
	selfID string
}

func New() *Client {
	c := &Client{}
	if _, err := os.Stat(sockPath); err != nil {
		return c
	}
	c.http = &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", sockPath)
			},
		},
	}
	if host, err := os.Hostname(); err == nil {
		c.selfID = host
	}
	if err := c.ping(); err != nil {
		c.http = nil
		return c
	}
	return c
}

func (c *Client) Available() bool { return c != nil && c.http != nil }

func (c *Client) do(method, path string, body any) (*http.Response, error) {
	if !c.Available() {
		return nil, fmt.Errorf("docker: socket unavailable")
	}
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequest(method, "http://docker"+path, &buf)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.http.Do(req)
}

func decode(resp *http.Response, dst any) error {
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var msg struct {
			Message string `json:"message"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&msg)
		if msg.Message == "" {
			msg.Message = resp.Status
		}
		return fmt.Errorf("docker: %s", msg.Message)
	}
	if dst == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

func (c *Client) ping() error {
	resp, err := c.do("GET", "/v1.41/_ping", nil)
	if err != nil {
		return err
	}
	return decode(resp, nil)
}// inspect mirrors the Docker container-inspect fields we need.
type inspect struct {
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	Config struct {
		Image  string            `json:"Image"`
		Env    []string          `json:"Env"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State struct {
		Status string `json:"Status"`
	} `json:"State"`
	NetworkSettings struct {
		Networks map[string]struct {
			NetworkID string `json:"NetworkID"`
		} `json:"Networks"`
		Ports map[string][]struct {
			HostPort string `json:"HostPort"`
		} `json:"Ports"`
	} `json:"NetworkSettings"`
}

func envVal(env []string, key string) string {
	prefix := key + "="
	for _, e := range env {
		if strings.HasPrefix(e, prefix) {
			return strings.TrimPrefix(e, prefix)
		}
	}
	return ""
}

func (c *Client) inspect(id string) (*inspect, error) {
	resp, err := c.do("GET", "/v1.41/containers/"+id+"/json", nil)
	if err != nil {
		return nil, err
	}
	var in inspect
	if err := decode(resp, &in); err != nil {
		return nil, err
	}
	return &in, nil
}

func (c *Client) selfProject() (project, network, image string, err error) {
	in, err := c.inspect(c.selfID)
	if err != nil {
		return "", "", "", err
	}
	project = in.Config.Labels["com.docker.compose.project"]
	for name := range in.NetworkSettings.Networks {
		network = name
		break
	}
	image = in.Config.Image
	if project == "" || network == "" || image == "" {
		err = fmt.Errorf("docker: self container is not a compose service (project=%q network=%q image=%q)",
			project, network, image)
	}
	return project, network, image, err
}

func toNode(in *inspect) NodeContainer {
	n := NodeContainer{
		ContainerID: in.ID[:12],
		Name:        strings.TrimPrefix(in.Name, "/"),
		NodeID:      envVal(in.Config.Env, "ORBIX_NODE_ID"),
		State:       in.State.Status,
		Image:       in.Config.Image,
	}
	if n.NodeID == "" {
		n.NodeID = in.Config.Labels["orbix.node.id"]
	}
	for port, bindings := range in.NetworkSettings.Ports {
		if strings.HasPrefix(port, "8080/") && len(bindings) > 0 {
			n.HostPort = bindings[0].HostPort
		}
	}
	return n
}

func (c *Client) ListNodes() ([]NodeContainer, error) {
	project, _, _, err := c.selfProject()
	if err != nil {
		return nil, err
	}
	resp, err := c.do("GET", "/v1.41/containers/json?all=1&filters=%7B%22label%22%3A%5B%22com.docker.compose.project%3D"+project+"%22%5D%7D", nil)
	if err != nil {
		return nil, err
	}
	var list []struct {
		ID string `json:"Id"`
	}
	if err := decode(resp, &list); err != nil {
		return nil, err
	}
	out := make([]NodeContainer, 0, len(list))
	for _, b := range list {
		in, err := c.inspect(b.ID)
		if err != nil {
			continue
		}
		n := toNode(in)
		if n.NodeID == "" {
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

func (c *Client) findContainer(nodeID string) (string, error) {
	nodes, err := c.ListNodes()
	if err != nil {
		return "", err
	}
	for _, n := range nodes {
		if n.NodeID == nodeID || n.Name == nodeID ||
			strings.HasSuffix(n.Name, "-"+nodeID) || strings.HasSuffix(n.Name, "_"+nodeID) {
			return n.ContainerID, nil
		}
	}
	return "", fmt.Errorf("docker: no container for node %q", nodeID)
}

func (c *Client) KillNode(nodeID string) error {
	id, err := c.findContainer(nodeID)
	if err != nil {
		return err
	}
	resp, err := c.do("POST", "/v1.41/containers/"+id+"/kill", nil)
	if err != nil {
		return err
	}
	return decode(resp, nil)
}

func (c *Client) AddNode(nodeID string) (*NodeContainer, error) {
	project, network, image, err := c.selfProject()
	if err != nil {
		return nil, err
	}
	if existing, err := c.findContainer(nodeID); err == nil {
		return nil, fmt.Errorf("docker: node %q already exists as container %s (start it instead of adding)", nodeID, existing)
	}
	body := map[string]any{
		"Image":    image,
		"Hostname": nodeID,
		"Env": []string{
			"ORBIX_NODE_ID=" + nodeID,
			"ORBIX_ETCD=http://etcd:2379",
			"ORBIX_LISTEN=:8080",
			"ORBIX_DATA_DIR=/data",
		},
		"Labels": map[string]string{
			"orbix.node":                  "1",
			"orbix.node.id":               nodeID,
			"com.docker.compose.project": project,
		},
		"ExposedPorts": map[string]any{"8080/tcp": map[string]any{}},
		"HostConfig": map[string]any{
			"NetworkMode":      network,
			"PublishAllPorts":  true,
			"RestartPolicy":    map[string]any{"Name": "unless-stopped"},
		},
	}
	resp, err := c.do("POST", "/v1.41/containers/create?name="+nodeID, body)
	if err != nil {
		return nil, err
	}
	var created struct {
		ID string `json:"Id"`
	}
	if err := decode(resp, &created); err != nil {
		return nil, err
	}
	if resp, err := c.do("POST", "/v1.41/containers/"+created.ID+"/start", nil); err != nil {
		return nil, err
	} else if err := decode(resp, nil); err != nil {
		return nil, err
	}
	in, err := c.inspect(created.ID)
	if err != nil {
		return nil, err
	}
	n := toNode(in)
	return &n, nil
}
func (c *Client) StartNode(nodeID string) error {
	id, err := c.findContainer(nodeID)
	if err != nil {
		return err
	}
	resp, err := c.do("POST", "/v1.41/containers/"+id+"/start", nil)
	if err != nil {
		return err
	}
	return decode(resp, nil)
}