package main

import (
	"github.com/Imm0bilize/gunshot-core-service/internal/app"
	"github.com/Imm0bilize/gunshot-core-service/internal/config"
	"log"
)

func main() {
	cfg, err := config.New(".env.public")
	if err != nil {
		log.Fatal(err)
	}

	app.Run(cfg)
}
