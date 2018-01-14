package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	flag "github.com/spf13/pflag"
)

const (
	//VERSION info
	VERSION = "v0.1.0"
)

var (
	depth     int
	data      bool
	rootPath  string
	zkServers []string

	version bool
	help    bool
)

type nodeInfo struct {
	path  string
	level int
	stat  zk.Stat
	data  []byte //data of znode
}

func init() {

	flag.IntVar(&depth, "depth", 0, "list depth of directory deep, default is 0 for recursively to the leaf.")
	flag.BoolVar(&data, "data", false, "output the data of znode")
	flag.StringVar(&rootPath, "root-path", "/", "the root path list from, the path should be start with '/'")
	flag.StringSliceVar(&zkServers, "zk", []string{"127.0.0.1:2181"}, "zk address")
	flag.BoolVar(&help, "help", false, "print help and exit")
	flag.BoolVar(&version, "version", false, "print version info")

}

func printVersion() {
	fmt.Printf("zktree: is a tool list zookeeper node contents of directories in a tree-like format， version: %s\n", VERSION)
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

	if strings.Index(rootPath, "/") != 0 {
		panic("root path is not start with '/' ")
	}
	nis, err := zkWalker(c, rootPath, 0, depth)
	if err != nil {
		fmt.Printf("err:%s\n", err.Error())
	}
	printNodeInfo(nis)
}

func zkWalker(c *zk.Conn, path string, level, depth int) ([]*nodeInfo, error) {
	if c == nil {
		return nil, nil
	}

	p := filepath.Clean(path)
	d, stat, err := c.Get(p)
	if err != nil {
		return nil, fmt.Errorf("get zk value node failed %s", err.Error())
	}

	var nodes []*nodeInfo
	node := nodeInfo{
		path:  p,
		level: level,
		data:  d,
		stat:  *stat,
	}
	nodes = append(nodes, &node)

	level++
	if depth != 0 && level > depth {
		return nodes, nil
	}
	if stat.NumChildren != 0 {
		children, _, err := c.Children(p)
		if err != nil {
			return nil, fmt.Errorf("get children node failed, path:%s err: %s", p, err.Error())
		}
		var nis []*nodeInfo
		for _, child := range children {
			path := filepath.Clean(path + "/" + child)
			nis, err = zkWalker(c, path, level, depth)
			if err != nil {
				fmt.Printf("zk walker failed: %s, err: %s", path, err.Error())
				continue
			}
			nodes = append(nodes, nis...)
		}
	}

	return nodes, nil
}

func printNodeInfo(nis []*nodeInfo) {
	if nis == nil || len(nis) == 0 {
		fmt.Println("no children node found")
		return
	}

	sort.Slice(nis, func(i, j int) bool {
		a := nis[i].path
		b := nis[j].path
		return strings.Compare(a, b) == -1
	})

	for _, ni := range nis {
		if data {
			fmt.Printf("level:%d\t%s\t\t%+v\n", ni.level, ni.path, string(ni.data))
		} else {
			fmt.Printf("level:%d\t%s\n", ni.level, ni.path)
		}
	}
}
