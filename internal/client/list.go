package client

import (
	"context"
	"fmt"
	"log"
	"os"
	"text/tabwriter"

	"github.com/MatteoZacca/Fractal/pb"
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all files currently stored in the Fractal cluster",
	Run: func(cmd *cobra.Command, args []string) {
		// Connect to the NameNode
		masterClient, conn, err := getNameNodeClient()
		if err != nil {
			log.Fatalf("Failed to connect to NameNode: %v", err)
		}
		defer conn.Close()

		res, err := masterClient.ListFiles(context.Background(), &pb.ListFilesRequest{})
		if err != nil {
			log.Fatalf("Failed to retrieve file list from NameNode: %v", err)
		}

		if len(res.Files) == 0 {
			fmt.Println("The Fractal cluster is currently empty.")
			return
		}

		fmt.Println("\n🗄️  FRACTAL DISTRIBUTED FILE SYSTEM")
		fmt.Println("-------------------------------------------------")

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 4, ' ', 0)
		fmt.Fprintln(w, "FILENAME\tCHUNKS\tSTATUS")
		fmt.Fprintln(w, "--------\t------\t------")

		for _, file := range res.Files {
			fmt.Fprintf(w, "%s\t%d\t✅ HEALTHY\n", file.FileName, file.ChunkCount)
		}
		w.Flush()
		fmt.Println("-------------------------------------------------")
	},
}

func init() {
	rootCmd.AddCommand(listCmd)
}
