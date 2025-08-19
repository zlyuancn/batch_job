package main

import (
	"github.com/zly-app/uapp"
)

func main() {
	app := uapp.NewApp("batch_job")
	defer app.Exit()

	app.Run()
}
