package main

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"os"

	"github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
)

const (
	// UnusedHashSlot ...
	UnusedHashSlot = iota
	// NewHashSlot ...
	NewHashSlot
	// AssignedHashSlot ...
	AssignedHashSlot
)

// NodeInfo detail info for redis node.
type NodeInfo struct {
	host string
	port uint

	name       string
	addr       string
	flags      []string
	replicate  string
	pingSent   int
	pingRecv   int
	weight     int
	balance    int
	linkStatus string
	slots      map[int]int
	migrating  map[int]string
	importing  map[int]string
}

// HasFlag if node has flag
func (n *NodeInfo) HasFlag(flag string) bool {
	for _, f := range n.flags {
		if strings.Contains(f, flag) {
			return true
		}
	}
	return false
}

// String stringify to host:port
func (n *NodeInfo) String() string {
	return fmt.Sprintf("%s:%d", n.host, n.port)
}

// ClusterNode struct of redis cluster node.
type ClusterNode struct {
	r             redis.Conn
	info          *NodeInfo
	dirty         bool
	friends       [](*NodeInfo)
	replicasNodes [](*ClusterNode)
	verbose       bool
}

// NewClusterNode Create a new Cluster Node
func NewClusterNode(addr string) (node *ClusterNode) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		logrus.Fatalf("New cluster node error: %s!", err)
		return nil
	}

	p, _ := strconv.ParseUint(port, 10, 0)
	node = &ClusterNode{
		r: nil,
		info: &NodeInfo{
			host:      host,
			port:      uint(p),
			slots:     make(map[int]int),
			migrating: make(map[int]string),
			importing: make(map[int]string),
			replicate: "",
		},
		dirty:   false,
		verbose: false,
	}

	if os.Getenv("ENV_MODE_VERBOSE") != "" {
		node.verbose = true
	}

	return node
}

// Host ...
func (c *ClusterNode) Host() string {
	return c.info.host
}

// Port ...
func (c *ClusterNode) Port() uint {
	return c.info.port
}

// Name ...
func (c *ClusterNode) Name() string {
	return c.info.name
}

// HasFlag ...
func (c *ClusterNode) HasFlag(flag string) bool {
	for _, f := range c.info.flags {
		if strings.Contains(f, flag) {
			return true
		}
	}
	return false
}

// Replicate ...
func (c *ClusterNode) Replicate() string {
	return c.info.replicate
}

// SetReplicate ...
func (c *ClusterNode) SetReplicate(nodeID string) {
	c.info.replicate = nodeID
	c.dirty = true
}

// Weight ...
func (c *ClusterNode) Weight() int {
	return c.info.weight
}

// SetWeight ...
func (c *ClusterNode) SetWeight(w int) {
	c.info.weight = w
}

// Balance ...
func (c *ClusterNode) Balance() int {
	return c.info.balance
}

// SetBalance ...
func (c *ClusterNode) SetBalance(balance int) {
	c.info.balance = balance
}

// Slots ...
func (c *ClusterNode) Slots() map[int]int {
	return c.info.slots
}

// Migrating ...
func (c *ClusterNode) Migrating() map[int]string {
	return c.info.migrating
}

// Importing ...
func (c *ClusterNode) Importing() map[int]string {
	return c.info.importing
}

// R ...
func (c *ClusterNode) R() redis.Conn {
	return c.r
}

// Info ...
func (c *ClusterNode) Info() *NodeInfo {
	return c.info
}

// IsDirty ...
func (c *ClusterNode) IsDirty() bool {
	return c.dirty
}

// Friends ...
func (c *ClusterNode) Friends() []*NodeInfo {
	return c.friends
}

// ReplicasNodes ...
func (c *ClusterNode) ReplicasNodes() []*ClusterNode {
	return c.replicasNodes
}

// AddReplicasNode ...
func (c *ClusterNode) AddReplicasNode(node *ClusterNode) {
	c.replicasNodes = append(c.replicasNodes, node)
}

// String ...
func (c *ClusterNode) String() string {
	return fmt.Sprintf("%s:%d", c.info.host, c.info.port)
}

// Connect ...
func (c *ClusterNode) Connect(abort bool) (err error) {
	if c.r != nil {
		return nil
	}

	addr := fmt.Sprintf("%s:%d", c.info.host, c.info.port)
	//client, err := redis.DialTimeout("tcp", addr, 0, 1*time.Second, 1*time.Second)
	client, err := redis.Dial("tcp", addr, redis.DialConnectTimeout(60*time.Second))
	if err != nil {
		if abort {
			logrus.Fatalf("Sorry, connect to node %s failed in abort mode!", addr)
		} else {
			logrus.Errorf("Sorry, can't connect to node %s!", addr)
			return err
		}
	}

	if _, err = client.Do("PING"); err != nil {
		if abort {
			logrus.Fatalf("Sorry, ping node %s failed in abort mode!", addr)
		} else {
			logrus.Errorf("Sorry, ping node %s failed!", addr)
			return err
		}
	}

	if c.verbose {
		logrus.Printf("Connecting to node %s OK", c.String())
	}

	c.r = client
	return nil
}

// Call ...
func (c *ClusterNode) Call(cmd string, args ...interface{}) (interface{}, error) {
	err := c.Connect(true)
	if err != nil {
		return nil, err
	}

	return c.r.Do(cmd, args...)
}

// Dbsize ...
func (c *ClusterNode) Dbsize() (int, error) {
	return redis.Int(c.Call("DBSIZE"))
}

// ClusterAddNode ...
func (c *ClusterNode) ClusterAddNode(addr string) (ret string, err error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return "", fmt.Errorf("bad format of host:port: %s", addr)
	}
	return redis.String(c.Call("CLUSTER", "meet", host, port))
}

// ClusterReplicateWithNodeID ...
func (c *ClusterNode) ClusterReplicateWithNodeID(nodeid string) (ret string, err error) {
	return redis.String(c.Call("CLUSTER", "replicate", nodeid))
}

// ClusterForgetNodeID ...
func (c *ClusterNode) ClusterForgetNodeID(nodeid string) (ret string, err error) {
	return redis.String(c.Call("CLUSTER", "forget", nodeid))
}

// ClusterNodeShutdown ...
func (c *ClusterNode) ClusterNodeShutdown() (err error) {
	c.r.Send("SHUTDOWN")
	if err = c.r.Flush(); err != nil {
		return err
	}
	return nil
}

// ClusterCountKeysInSlot ...
func (c *ClusterNode) ClusterCountKeysInSlot(slot int) (int, error) {
	return redis.Int(c.Call("CLUSTER", "countkeysinslot", slot))
}

// ClusterGetKeysInSlot ...
func (c *ClusterNode) ClusterGetKeysInSlot(slot int, pipeline int) (string, error) {
	return redis.String(c.Call("CLUSTER", "getkeysinslot", slot, pipeline))
}

// ClusterSetSlot ...
func (c *ClusterNode) ClusterSetSlot(slot int, cmd string) (string, error) {
	return redis.String(c.Call("CLUSTER", "setslot", slot, cmd, c.Name()))
}

// AssertCluster ...
func (c *ClusterNode) AssertCluster() bool {
	info, err := redis.String(c.Call("INFO", "cluster"))
	if err != nil ||
		!strings.Contains(info, "cluster_enabled:1") {
		return false
	}

	return true
}

// AssertEmpty ...
func (c *ClusterNode) AssertEmpty() bool {

	info, err := redis.String(c.Call("CLUSTER", "INFO"))
	db0, e := redis.String(c.Call("INFO", "db0"))
	db0 = strings.Trim(db0, " ")
	// Check if db0 is empty
	empty := db0 == "" || strings.Contains(db0, "db0:keys=0,expires")
	fmt.Println(strings.Trim(db0, " ") == "")
	if err != nil || !strings.Contains(info, "cluster_known_nodes:1") ||
		e != nil || !empty {
		logrus.Fatalf("Node %s is not empty. Either the node already knows other nodes (check with CLUSTER NODES) or contains some key in database 0.", c.String())
	}

	return true
}

// LoadInfo ...
func (c *ClusterNode) LoadInfo(getfriends bool) (err error) {
	var result string
	if result, err = redis.String(c.Call("CLUSTER", "NODES")); err != nil {
		return err
	}

	nodes := strings.Split(result, "\n")
	for _, val := range nodes {
		// name addr flags role ping_sent ping_recv link_status slots
		parts := strings.Split(val, " ")
		if len(parts) <= 3 {
			continue
		}

		sent, _ := strconv.ParseInt(parts[4], 0, 32)
		recv, _ := strconv.ParseInt(parts[5], 0, 32)
		host, port, _ := net.SplitHostPort(parts[1])
		p, _ := strconv.ParseUint(port, 10, 0)

		node := &NodeInfo{
			name:       parts[0],
			addr:       parts[1],
			flags:      strings.Split(parts[2], ","),
			replicate:  parts[3],
			pingSent:   int(sent),
			pingRecv:   int(recv),
			linkStatus: parts[6],

			host:      host,
			port:      uint(p),
			slots:     make(map[int]int),
			migrating: make(map[int]string),
			importing: make(map[int]string),
		}

		if parts[3] == "-" {
			node.replicate = ""
		}

		if strings.Contains(parts[2], "myself") {
			c.info = node
			for i := 8; i < len(parts); i++ {
				slots := parts[i]
				if strings.Contains(slots, "<") {
					slotStr := strings.Split(slots, "-<-")
					slotID, _ := strconv.Atoi(slotStr[0])
					c.info.importing[slotID] = slotStr[1]
				} else if strings.Contains(slots, ">") {
					slotStr := strings.Split(slots, "->-")
					slotID, _ := strconv.Atoi(slotStr[0])
					c.info.migrating[slotID] = slotStr[1]
				} else if strings.Contains(slots, "-") {
					slotStr := strings.Split(slots, "-")
					firstID, _ := strconv.Atoi(slotStr[0])
					lastID, _ := strconv.Atoi(slotStr[1])
					c.AddSlots(firstID, lastID)
				} else {
					firstID, _ := strconv.Atoi(slots)
					c.AddSlots(firstID, firstID)
				}
			}
		} else if getfriends {
			c.friends = append(c.friends, node)
		}
	}
	return nil
}

// AddSlots ...
func (c *ClusterNode) AddSlots(start, end int) {
	for i := start; i <= end; i++ {
		c.info.slots[i] = NewHashSlot
	}
	c.dirty = true
}

// FlushNodeConfig ...
func (c *ClusterNode) FlushNodeConfig() {
	if !c.dirty {
		return
	}

	if c.Replicate() != "" {
		// run replicate cmd
		if _, err := c.ClusterReplicateWithNodeID(c.Replicate()); err != nil {
			// If the cluster did not already joined it is possible that
			// the slave does not know the master node yet. So on errors
			// we return ASAP leaving the dirty flag set, to flush the
			// config later.
			return
		}
	} else {
		// TODO: run addslots cmd
		var array []int
		for s, value := range c.Slots() {
			if value == NewHashSlot {
				array = append(array, s)
				c.info.slots[s] = AssignedHashSlot
			}
			c.ClusterAddSlots(array)
		}
	}

	c.dirty = false
}

// ClusterAddSlots check the error for call CLUSTER addslots
func (c *ClusterNode) ClusterAddSlots(args ...interface{}) (ret string, err error) {
	return redis.String(c.Call("CLUSTER", "addslots", args))
}

// ClusterDelSlots check the error for call CLUSTER delslots
func (c *ClusterNode) ClusterDelSlots(args ...interface{}) (ret string, err error) {
	return redis.String(c.Call("CLUSTER", "delslots", args))
}

// ClusterBumpepoch ...
func (c *ClusterNode) ClusterBumpepoch() (ret string, err error) {
	return redis.String(c.Call("CLUSTER", "bumpepoch"))
}

// InfoString ...
func (c *ClusterNode) InfoString() (result string) {
	var role = "M"

	if !c.HasFlag("master") {
		role = "S"
	}

	keys := make([]int, 0, len(c.Slots()))

	for k := range c.Slots() {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	slotstr := MergeNumArray2NumRange(keys)

	if c.Replicate() != "" && c.dirty {
		result = fmt.Sprintf("S: %s %s", c.info.name, c.String())
	} else {
		// fix myself flag not the first element of []slots
		result = fmt.Sprintf("%s: %s %s\n\t   slots:%s (%d slots) %s",
			role, c.info.name, c.String(), slotstr, len(c.Slots()), strings.Join(c.info.flags[1:], ","))
	}

	if c.Replicate() != "" {
		result = result + fmt.Sprintf("\n\t   replicates %s", c.Replicate())
	} else {
		result = result + fmt.Sprintf("\n\t   %d additional replica(s)", len(c.replicasNodes))
	}

	return result
}

// GetConfigSignature ...
func (c *ClusterNode) GetConfigSignature() string {
	config := []string{}

	result, err := redis.String(c.Call("CLUSTER", "NODES"))
	if err != nil {
		return ""
	}

	nodes := strings.Split(result, "\n")
	for _, val := range nodes {
		parts := strings.Split(val, " ")
		if len(parts) <= 3 {
			continue
		}

		sig := parts[0] + ":"

		slots := []string{}
		if len(parts) > 7 {
			for i := 8; i < len(parts); i++ {
				p := parts[i]
				if !strings.Contains(p, "[") {
					slots = append(slots, p)
				}
			}
		}
		if len(slots) == 0 {
			continue
		}
		sort.Strings(slots)
		sig = sig + strings.Join(slots, ",")

		config = append(config, sig)
	}

	sort.Strings(config)
	return strings.Join(config, "|")
}

///////////////////////////////////////////////////////////

// ClusterArray some useful struct contains cluster node.
type ClusterArray []ClusterNode

func (c ClusterArray) Len() int {
	return len(c)
}

func (c ClusterArray) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c ClusterArray) Less(i, j int) bool {
	return len(c[i].Slots()) < len(c[j].Slots())
}

// MovedNode ...
type MovedNode struct {
	Source ClusterNode
	Slot   int
}
