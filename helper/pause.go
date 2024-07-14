package helper

import (
	"bufio"
	"fmt"
	"os"
)

func Pause() {
	fmt.Println("Next...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}
