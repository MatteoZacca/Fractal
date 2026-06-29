package main

import (
	"github.com/MatteoZacca/Fractal/internal/client"
	"github.com/spf13/cobra"
)

var readCmd = &cobra.Command{
	Use:   "read [path\\to\\file\\in\\docker]",
	Short: "Download and reassemble a file from the Fractal cluster",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fileName := args[0]
		client.DownloadFile(fileName)
	},
}

func init() {
	rootCmd.AddCommand(readCmd)
}
