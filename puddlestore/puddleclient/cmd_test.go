package puddleclient

import (
	"fmt"
	"testing"

	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/raft/raft"
	"github.com/brown-csci1380/YimingLiBrown-YuqiChai-s19/puddlestore/tapestry/tapestry"
)


func TestEverything(t *testing.T) {
	raft.SuppressLoggers()
	tapestry.SuppressLoggers()

	pc := CreateClient("0.0.0.1")
	pc.Create("raft", 3)
	// time.Sleep(time.Second * 4)
	pc.Create("tapestry", 5)
	fmt.Println("**************FINISH INITIALIZATION****************")

	err := pc.Mk("f1")
	if err != nil {
		t.Error(err)
	}
	err = pc.Mkdir("d1")
	if err != nil {
		t.Error(err)
	}
	err = pc.CdRelative("d1")
	if err != nil {
		t.Error(err)
	}
	err = pc.Mkdir("d3")
	if err != nil {
		t.Error(err)
	}
	err = pc.Mk("f2")
	if err != nil {
		t.Error(err)
	}
	err = pc.CdAbsolute("/")
	if err != nil {
		t.Error(err)
	}

	err = pc.Write("f1", 0, []byte("Hllo"))
	if err != nil {
		t.Error(err)
	}

	//////////////////////////
	b, err := pc.Read("f1", 0)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("Content: %v\n", string(b))
	err = pc.Disp("f1")
	if err != nil {
		t.Error(err)
	}
	_, err = pc.Ls()
	if err != nil {
		t.Error(err)
	}
	err = pc.Mkdir("d2")
	if err != nil {
		t.Error(err)
	}
	err = pc.Rmdir("d2")
	if err != nil {
		t.Error(err)
	}
	err = pc.Rm("f1")
	if err != nil {
		t.Error(err)
	}
	err = pc.Rmdir("d1")
	if err != nil {
		t.Error(err)
	}
	_, err = pc.AddTnode("6543")
	if err != nil {
		t.Error(err)
	}
	_, err = pc.RemoveTnodes("2")
	if err != nil {
		t.Error(err)
	}
	fmt.Println("**************BELOW ARE SOME ACTIONS THAT ARE NOT PERMITTED****************")

	err = pc.CdAbsolute("/d1/d3")
	if err == nil {
		t.Errorf("Parent folder should already been deleted\n")
	}
	err = pc.CdRelative("d1/d3")
	if err == nil {
		t.Errorf("Parent folder should already been deleted\n")
	}
	_, err = pc.Read("d1/f2", 0)
	if err == nil {
		t.Errorf("Parent folder should already been deleted\n")
	}

=======
// func TestMKdir(t *testing.T) {
// 	raft.SuppressLoggers()
// 	pc := CreateClient("0.0.0.1")
// 	// time.Sleep(time.Second * 4)
// 	children, paths, err := pc.Create("raft", 3)
// 	if err != nil {
// 		fmt.Printf("fail to create raft clusters: %v\n", err)
// 		return
// 	}
//
// 	// fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!rclient: %v\n", pc.RClient)
// 	fmt.Printf("raft nodes in zookeeper: %v, table: %v\n", children, paths)
// 	time.Sleep(time.Second * 4)
// 	childrenT, pathsT, err := pc.Create("tapestry", 3)
// 	if err != nil {
// 		fmt.Printf("fail to create tapestry clusters: %v\n", err)
// 		return
// 	}
// 	// // fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!Tclient: %v\n", pc.TClient)
// 	fmt.Printf("tap nodes in zookeeper: %v, table: %v\n", childrenT, pathsT)
//
// 	fmt.Printf("table: %v\n", pc.Table)
// 	err = pc.Mkdir("lym")
// 	if err != nil {
// 		fmt.Printf("fail to make this dir: %v\n", err)
// 		return
// 	}
// 	pc.CdAbsolute("/lym")
// 	res, _ := pc.Pwd()
// 	fmt.Printf("PATH NOW: %v\n", res)
//
// 	pc.CdAbsolute("/")
// 	pc.Rmdir("lym")
// 	list, _ := pc.Ls()
// 	fmt.Printf("ls: %v\n", list)
// }
//
// func TestMK(t *testing.T) {
// 	raft.SuppressLoggers()
// 	pc := CreateClient("0.0.0.1")
// 	// time.Sleep(time.Second * 4)
// 	children, paths, err := pc.Create("raft", 3)
// 	if err != nil {
// 		fmt.Printf("fail to create raft clusters: %v\n", err)
// 		return
// 	}
//
// 	// fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!rclient: %v\n", pc.RClient)
// 	fmt.Printf("raft nodes in zookeeper: %v, table: %v\n", children, paths)
// 	time.Sleep(time.Second * 4)
// 	childrenT, pathsT, err := pc.Create("tapestry", 3)
// 	if err != nil {
// 		fmt.Printf("fail to create tapestry clusters: %v\n", err)
// 		return
// 	}
// 	// // fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!Tclient: %v\n", pc.TClient)
// 	fmt.Printf("tap nodes in zookeeper: %v, table: %v\n", childrenT, pathsT)
//
// 	fmt.Printf("table: %v\n", pc.Table)
//
// 	// err = pc.Mkdir("app")
// 	// if err != nil {
// 	// 	fmt.Printf("fail to make this app dir: %v\n", err)
// 	// 	return
// 	// }
// 	err = pc.Mk("lym")
// 	if err != nil {
// 		fmt.Printf("fail to make this file: %v\n", err)
// 		return
// 	}
// 	list, err := pc.Ls()
// 	if err != nil {
// 		fmt.Printf("err when list: %v\n", err)
// 	}
// 	fmt.Printf("list: %v\n", list)
// 	// pc.Rm("lym")
// 	// list2, err := pc.Ls()
// 	// if err != nil {
// 	// 	fmt.Printf("err when list: %v\n", err)
// 	// }
// 	// fmt.Printf("list: %v\n", list2)
//
// }
//
// func TestLs(t *testing.T) {
// 	raft.SuppressLoggers()
// 	pc := CreateClient("0.0.0.1")
// 	// time.Sleep(time.Second * 4)
// 	children, paths, err := pc.Create("raft", 3)
// 	if err != nil {
// 		fmt.Printf("fail to create raft clusters: %v\n", err)
// 		return
// 	}
//
// 	// fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!rclient: %v\n", pc.RClient)
// 	fmt.Printf("raft nodes in zookeeper: %v, table: %v\n", children, paths)
// 	time.Sleep(time.Second * 4)
// 	childrenT, pathsT, err := pc.Create("tapestry", 3)
// 	if err != nil {
// 		fmt.Printf("fail to create tapestry clusters: %v\n", err)
// 		return
// 	}
// 	// // fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!Tclient: %v\n", pc.TClient)
// 	fmt.Printf("tap nodes in zookeeper: %v, table: %v\n", childrenT, pathsT)
// 	fmt.Println("**************FINISH INITIALIZATION****************")
// 	err = pc.Mkdir("lym")
// 	if err != nil {
// 		fmt.Printf("fail to make this dir: %v\n", err)
// 		return
// 	}
//
// 	pc.Mkdir("dir")
// 	pc.CdRelative("dir")
// 	pc.Mk("file")
//
// 	pc.Write("file", 0, []byte("dfiqoifnioqjoivnqo"))
// 	pc.CdAbsolute("/")
// 	b2, err := pc.Read("dir/file", 0)
//
// 	if err != nil {
// 		fmt.Printf("^^^^^^AFTERREAD1 err: %v", err)
// 	}
// 	fmt.Printf("CONTENT: %v\n", string(b2))
//
// 	pc.Rmdir("dir")
// 	b, err := pc.Read("dir/file", 0)
//
// 	if err != nil {
// 		fmt.Printf("^^^^^^AFTERREAD err: %v", err)
// 	}
// 	fmt.Printf("CONTENT: %v\n", string(b))
//
// }
//
// func TestCd(t *testing.T) {
// 	raft.SuppressLoggers()
// 	tapestry.SuppressLoggers()
// 	pc := CreateClient("0.0.0.1")
// 	// time.Sleep(time.Second * 4)
// 	children, paths, err := pc.Create("raft", 3)
// 	if err != nil {
// 		fmt.Printf("fail to create raft clusters: %v\n", err)
// 		return
// 	}
//
// 	// fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!rclient: %v\n", pc.RClient)
// 	fmt.Printf("raft nodes in zookeeper: %v, table: %v\n", children, paths)
// 	time.Sleep(time.Second * 4)
// 	childrenT, pathsT, err := pc.Create("tapestry", 3)
// 	if err != nil {
// 		fmt.Printf("fail to create tapestry clusters: %v\n", err)
// 		return
// 	}
// 	// // fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!Tclient: %v\n", pc.TClient)
// 	fmt.Printf("tap nodes in zookeeper: %v, table: %v\n", childrenT, pathsT)
//
// 	fmt.Printf("table: %v\n", pc.Table)
//
// 	// err = pc.Mkdir("app")
// 	// if err != nil {
// 	// 	fmt.Printf("fail to make this app dir: %v\n", err)
// 	// 	return
// 	// }
// 	pc.Mk("fqwh")
// 	list, _ := pc.Ls()
// 	fmt.Printf("$$$$$$$$$$$$$$$list1: %v\n", list)
// 	err = pc.Mkdir("lym")
// 	if err != nil {
// 		fmt.Printf("fail to make this file: %v\n", err)
// 		return
// 	}
// 	list2, _ := pc.Ls()
// 	fmt.Printf("$$$$$$$$$$$$$$$list2: %v\n", list2)
//
// 	pc.Mkdir("cyq")
// 	list3, _ := pc.Ls()
// 	fmt.Printf("$$$$$$$$$$$$$$$list3: %v\n", list3)
//
// 	// pc.Rm("lym")
// 	// list2, err := pc.Ls()
// 	// if err != nil {
// 	// 	fmt.Printf("err when list: %v\n", err)
// 	// }
// 	// fmt.Printf("list: %v\n", list2)
//
// }
//
// func TestEverything(t *testing.T) {
// 	raft.SuppressLoggers()
// 	tapestry.SuppressLoggers()
// 	pc := CreateClient("0.0.0.1")
// 	pc.Create("raft", 3)
// 	time.Sleep(time.Second * 4)
// 	pc.Create("tapestry", 3)
// 	fmt.Println("**************FINISH INITIALIZATION****************")
// 	pc.Mk("f1")
// 	pc.Write("f1", 0, []byte("fk u"))
// 	b, _ := pc.Read("f1", 0)
// 	fmt.Printf("CONTENT: %v\n", string(b))
// 	pc.Disp("f1")
// 	fmt.Println("******************************")
// 	pc.Mkdir("d1")
// 	pc.CdRelative("d1")
// 	pc.Mkdir("d2")
// 	pc.CdRelative("d2")
// 	pc.Mk("f2")
// 	pc.Write("f2", 0, []byte("fk 2"))
// 	pc.Ls()
// 	pc.Pwd()
//
// 	pc.CdAbsolute("/")
// 	fmt.Println("****************1**************")
// 	//pc.Read("d1/d2/f2", 0)
// 	fmt.Println("****************1**************")
// 	pc.CdAbsolute("f2")
// 	fmt.Println("****************2**************")
// 	pc.Rm("f1")
// 	pc.Rmdir("d1")
// 	fmt.Println("******************************")
// 	pc.CdAbsolute("/d1")
// 	pc.CdRelative("d1")
// 	pc.Read("f1", 0)
// 	pc.Write("f1", 0, []byte("fk u 2"))
// 	pc.Disp("f1")
// }
//
// func TestF(t *testing.T) {
// 	raft.SuppressLoggers()
// 	tapestry.SuppressLoggers()
// 	pc := CreateClient("0.0.0.1")
// 	pc.Create("raft", 3)
// 	time.Sleep(time.Second * 4)
// 	pc.Create("tapestry", 3)
// 	fmt.Println("**************FINISH INITIALIZATION****************")
// 	// /f1
// 	pc.Mk("f1")
// 	pc.Write("f1", 0, []byte("fk u"))
// 	b, _ := pc.Read("f1", 0)
// 	fmt.Printf("CONTENT: %v\n", string(b))
// 	pc.Disp("f1")
// 	fmt.Println("******************************")
// 	pc.Mkdir("d1")
// 	pc.CdRelative("d1")
// 	// /d1/d2
// 	pc.Mkdir("d2")
// 	pc.CdRelative("d2")
// 	pc.Mk("f2")
// 	pc.Write("f2", 0, []byte("fk 2"))
// 	pc.Disp("f2")
// 	pc.Ls()
// 	pc.Pwd()
//
// 	pc.CdAbsolute("/")
// 	pwd, _ := pc.Pwd()
// 	fmt.Printf("***********pwd: %v\n", pwd)
// 	b22, _ := pc.Read("d1/d2/f2", 0)
// 	fmt.Printf("content: %v\n", string(b22))
//
// }

func TestRW(t *testing.T) {
	raft.SuppressLoggers()
	tapestry.SuppressLoggers()
	pc := CreateClient("0.0.0.1")
	pc.Create("raft", 3)
	time.Sleep(time.Second * 4)
	pc.Create("tapestry", 3)
	fmt.Println("**************FINISH INITIALIZATION****************")
	// /f1
	pc.Mk("f1")
	pc.Write("f1", 0, []byte("f2f4"))
	b, _ := pc.Read("f1", 0)
	fmt.Printf("CONTENT: %v\n", string(b))
	pc.Write("f1", 1, []byte("c123"))
	b1, _ := pc.Read("f1", 0)
	fmt.Printf("CONTENT: %v\n", string(b1))
	pc.Write("f1", 7, []byte("acd3g"))
	b2, _ := pc.Read("f1", 0)
	fmt.Printf("CONTENT: %v\n", string(b2))

}
