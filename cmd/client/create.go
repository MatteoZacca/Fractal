package main

import (
	"log"
	"path/filepath"

	"github.com/MatteoZacca/Fractal/internal/client"
	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:   "create [path\\to\\local\\file]",
	Short: "create and upload a file to the cluster",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		localPath := args[0]
		targetFileName := filepath.Base(localPath)

		err := client.UploadFile(localPath, targetFileName)
		if err != nil {
			log.Fatalf("[create] command failed: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(createCmd)
}
