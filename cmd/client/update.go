package main

import (
	"log"
	"path/filepath"

	"github.com/MatteoZacca/Fractal/internal/client"
	"github.com/spf13/cobra"
)

var updateCmd = &cobra.Command{
	Use:   "update [path\\to\\local\\file]",
	Short: "Overwrite an existing file",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		localPath := args[0]
		dockerFileName := filepath.Base(localPath)

		err := client.UpdateFile(localPath, dockerFileName)
		if err != nil {
			log.Fatalf("[update] command failed: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(updateCmd)
}
