package main

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type nodeInfo struct {
	path  string
	value string
	level int
}

func main() {
	c, _, err := zk.Connect([]string{"127.0.0.1"}, time.Second*10, zk.WithLogInfo(false))
	if err != nil {
		panic(err)
	}

	nis, err := getChildren(c, "/", 0, 4)
	if err != nil {
		fmt.Printf("err:%s\n", err.Error())
	}
	printNodeInfo(nis)
}

func getChildren(c *zk.Conn, path string, level, depth int) ([]*nodeInfo, error) {
	if c == nil {
		return nil, nil
	}

	p := filepath.Clean(path)
	children, stat, err := c.Children(p)
	if err != nil {
		return nil, fmt.Errorf("get zk children node failed, path:%s err: %s\n", p, err.Error())
	}

	var nodes []*nodeInfo
	v, _, err := c.Get(path)
	if err != nil {
		return nil, fmt.Errorf("get zk children node failed %s", err.Error())
	}

	var node nodeInfo
	node.path = path
	node.level = level
	node.value = string(v)
	nodes = append(nodes, &node)

	level++
	if level > depth {
		return nodes, nil
	}
	if stat.NumChildren != 0 {
		var nis []*nodeInfo
		for _, child := range children {
			path := filepath.Clean(path + "/" + child)
			nis, err = getChildren(c, path, level, depth)
			if err != nil {
				fmt.Printf("failed to get children info of node: %s, err: %s\n", path, err.Error())
				continue
			}
			nodes = append(nodes, nis...)
		}
	}

	return nodes, nil
}

func printNodeInfo(nis []*nodeInfo) {
	if nis == nil || len(nis) == 0 {
		fmt.Printf("no children node found")
		return
	}

	for _, ni := range nis {
		fmt.Printf("level:%d\t%s\t\t%s\n", ni.level, ni.path, ni.value)
	}
}
