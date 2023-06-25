package main

import (
	"errors"
	"fmt"
	cid2 "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-ipfs-ds-help"
	"github.com/spf13/cobra"
)

func parsecid() *cobra.Command {
	return &cobra.Command{
		Use:   "parsecid",
		Short: "cover cid to key",
		Long:  `cover cid to to storage key`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("error: parameter error! ")
			}

			//cid, err := cid2.Parse("QmVXmqg3rrv2p2QGP55qjexMm8JrryAwCF85MMViKAnMBG")
			cid, err := cid2.Parse(args[0])
			if err != nil {
				fmt.Println(err)
				return err
			}
			key := ds.NewKeyFromBinary(cid.Bytes())
			fmt.Printf("key: %s \n", key.String())

			fmt.Printf("dir: %s \n", getDir(key.String()))
			fmt.Printf("file: %s%s.data \n", getDir(key.String()), key.String())

			return nil
		},
	}
}

const suffixLen = 2

func getDir(noslash string) string {
	str := noslash
	offset := len(str) - suffixLen - 1
	return str[offset : offset+suffixLen]
}
