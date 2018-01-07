package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	flag "github.com/spf13/pflag"
)

const (
	//VERSION info
	VERSION = "v0.1.0"
)

var (
	zkServers []string
	version   bool
	help      bool
	depth     int
)

type nodeInfo struct {
	path  string
	value string
	level int
}

func init() {
	flag.StringSliceVar(&zkServers, "zk", []string{"127.0.0.1"}, "zk address")
	flag.BoolVar(&version, "version", false, "print version info")
	flag.BoolVar(&help, "help", false, "print help and exit")
	flag.IntVar(&depth, "depth", 0, "list depth of directory deep, default is 0 for recursively to the leaf.")

}

func printVersion() {
	fmt.Printf("zktree: is a tool list zookeeper node contents of directories in a tree-like format， version: %s", VERSION)
}

func main() {
	flag.Parse()
	if version {
		printVersion()
		os.Exit(1)
	}
	if help {
		flag.Usage()
		os.Exit(1)
	}

	c, _, err := zk.Connect(zkServers, time.Second*10, zk.WithLogInfo(false))
	if err != nil {
		panic(err)
	}

	nis, err := getChildren(c, "/", 0, depth)
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
	if depth != 0 && level > depth {
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
