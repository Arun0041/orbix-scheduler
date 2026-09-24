package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type NodeView struct {
	NodeID     string `json:"node_id"`
	Addr       string `json:"addr,omitempty"`
	JoinedAt   string `json:"joined_at,omitempty"`
	Leader     bool   `json:"leader"`
	Member     bool   `json:"member"`
	State      string `json:"state"`
	HostPort   string `json:"host_port,omitempty"`
	Manageable bool   `json:"manageable"`
}

func (s *Server) listNodes(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	members, _ := s.node.Members(ctx)
	lead := s.node.Leader()
	byID := map[string]*NodeView{}
	for _, m := range members {
		byID[m.NodeID] = &NodeView{
			NodeID:   m.NodeID,
			Addr:     m.Addr,
			JoinedAt: m.JoinedAt,
			Leader:   lead.NodeID != "" && m.NodeID == lead.NodeID,
			Member:   true,
		}
	}
	dockAvail := s.dock.Available()
	if dockAvail {
		if containers, err := s.dock.ListNodes(); err == nil {
			for _, dc := range containers {
				v, ok := byID[dc.NodeID]
				if !ok {
					v = &NodeView{NodeID: dc.NodeID}
					byID[dc.NodeID] = v
				}
				v.State = dc.State
				v.HostPort = dc.HostPort
				v.Manageable = true
			}
		} else {
			dockAvail = false
		}
	}
	out := make([]*NodeView, 0, len(byID))
	for _, v := range byID {
		if v.State == "" {
			v.State = "external"
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	writeJSON(w, http.StatusOK, map[string]any{
		"nodes":  out,
		"docker": dockAvail,
		"self":   s.node.ID,
	})
}

var nodeIDRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)

func (s *Server) nextNodeID(ctx context.Context) string {
	max := 0
	consider := func(id string) {
		if strings.HasPrefix(id, "node") {
			if n, err := strconv.Atoi(strings.TrimPrefix(id, "node")); err == nil && n > max {
				max = n
			}
		}
	}
	if members, err := s.node.Members(ctx); err == nil {
		for _, m := range members {
			consider(m.NodeID)
		}
	}
	if s.dock.Available() {
		if containers, err := s.dock.ListNodes(); err == nil {
			for _, dc := range containers {
				consider(dc.NodeID)
			}
		}
	}
	return fmt.Sprintf("node%d", max+1)
}// addNode creates and starts a fresh node container on the demo network.
func (s *Server) addNode(w http.ResponseWriter, r *http.Request) {
	if !s.dock.Available() {
		writeErr(w, http.StatusNotImplemented,
			fmt.Errorf("adding nodes needs the docker socket: mount /var/run/docker.sock into the node container"))
		return
	}
	var body struct {
		NodeID string `json:"node_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	id := strings.TrimSpace(body.NodeID)
	if id == "" {
		id = s.nextNodeID(ctx)
	}
	if !nodeIDRe.MatchString(id) {
		writeErr(w, http.StatusBadRequest,
			fmt.Errorf("node_id must match [a-z0-9-], up to 32 chars"))
		return
	}
	n, err := s.dock.AddNode(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"node_id":   n.NodeID,
		"container": n.ContainerID,
		"host_port": n.HostPort,
		"status":    "started",
	})
}

func (s *Server) killNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("node id is required"))
		return
	}
	if s.dock.Available() {
		if err := s.dock.KillNode(id); err != nil {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{
			"killed": id,
			"mode":   "sigkill",
		})
		return
	}
	if id == s.node.ID {
		s.leaveNode(w, r)
		return
	}
	writeErr(w, http.StatusNotImplemented,
		fmt.Errorf("killing remote nodes needs the docker socket: mount /var/run/docker.sock"))
}

func (s *Server) leaveNode(w http.ResponseWriter, r *http.Request) {
	if s.onLeave == nil {
		writeErr(w, http.StatusNotImplemented, fmt.Errorf("graceful leave is not wired"))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"leaving": s.node.ID,
		"mode":    "graceful",
	})
	go s.onLeave()
}// startNode (re)starts an exited container, reviving killed nodes from the dashboard.
func (s *Server) startNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("node id is required"))
		return
	}
	if !s.dock.Available() {
		writeErr(w, http.StatusNotImplemented,
			fmt.Errorf("starting nodes needs the docker socket: mount /var/run/docker.sock into the node container"))
		return
	}
	if err := s.dock.StartNode(id); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"started": id,
	})
}