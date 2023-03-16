package main


import "os"
import "fmt"

func main(){
	oname := "mr-out-1"
	tmp_file := "mr-tmp-1"
	tfile, _ := os.OpenFile(tmp_file, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	
	fmt.Fprintf(tfile, "%v %v\n", "xixi", "xixi")
	
	os.Rename(tmp_file,oname)
}