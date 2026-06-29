package main

import (
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

		client.UpdateFile(localPath, dockerFileName)
	},
}

func init() {
	rootCmd.AddCommand(updateCmd)
}
