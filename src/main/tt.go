package main

import (
	"fmt"
	"sync"
	"time"
)

type Raft struct {
	Mu sync.Mutex
	Val int
}

func (rf *Raft) set(val int,pos int){
	fmt.Println("aa")
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	for i:=0;i<=10;i++{
		fmt.Println(i)
	}

}

func tt(){
	raft:=Raft{Val:1}


	for i:=0;i<=10;i++{
		go raft.set(i,i)
	}


	raft.Mu.Lock()
	fmt.Println("11")
	raft.Val=11


	time.Sleep(1000*time.Millisecond)

}


func main(){
	tt()
}