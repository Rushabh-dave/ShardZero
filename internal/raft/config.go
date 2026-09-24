package raft

import (
	"encoding/json"
	"errors"
	"io"
	"os"
)

type Membership struct {
	ClusterID string `json:"clusterId"`
	Peers     []Peer `json:"peers"`
}

func LoadMembership(path string) (Membership, error) {
	var m Membership
	f, err := os.Open(path)
	if err != nil {
		return m, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return m, err
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return m, errors.New("invalid trailing membership data")
	}
	return m, nil
}
