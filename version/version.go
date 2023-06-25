/*
Copyright athenasoft Corp. All Rights Reserved.

*/

package version

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

var Version string = "0.0.8"
var CommitSHA string = ""
var BaseVersion string = "0.0.8"
var BaseDockerLabel string = "com.mty.wang"
var DockerNamespace string = "mtstorage"
var BaseDockerNamespace string = "mtstorage"
var BuildDate string = "1970.1.1"

// Program name
var ProgramName = "node"

// Cmd returns the Cobra Command for Version
func Cmd() *cobra.Command {
	return cobraCommand
}

var cobraCommand = &cobra.Command{
	Use:   "version",
	Short: "Print version.",
	Long:  `Print current version.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 0 {
			return fmt.Errorf("trailing args detected")
		}
		// Parsing of the command line is done so silence cmd usage
		cmd.SilenceUsage = true
		fmt.Print(GetInfo())
		return nil
	},
}

// GetInfo returns version information for the peer
func GetInfo() string {
	if Version == "" {
		Version = "development build"
	}

	if CommitSHA == "" {
		CommitSHA = "development build"
	}

	return fmt.Sprintf("%s:\n Version: %s\n Commit SHA: %s\n Go version: %s\n"+
		" OS/Arch: %s\n BuildDate: %s\n",
		ProgramName,
		Version,
		CommitSHA,
		runtime.Version(),
		fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		BuildDate)
}
