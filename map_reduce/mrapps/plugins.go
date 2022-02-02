package mrapps

import "6.824labs/map_reduce/mr"

var MrMaps = make(map[string]mr.MapReduce)

func init() {
	MrMaps["wc"] = &WcMapReduce{}
}
