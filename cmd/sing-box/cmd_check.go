package main

import (
	"context"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"

	"github.com/spf13/cobra"
)

var checkStartGeph bool

func init() {
	mainCommand.AddCommand(commandCheck)
	commandCheck.Flags().BoolVar(&checkStartGeph, "start-geph", false, "launch Geph endpoints and validate control port availability")
}

var commandCheck = &cobra.Command{
	Use:   "check",
	Short: "Check configuration",
	Run: func(cmd *cobra.Command, args []string) {
		err := checkWithOptions(checkStartGeph)
		if err != nil {
			log.Fatal(err)
		}
	},
	Args: cobra.NoArgs,
}

func check() error {
	return checkWithOptions(false)
}

func checkWithOptions(startGeph bool) error {
	options, err := readConfigAndMerge()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(service.ExtendContext(globalCtx))
	instance, err := box.New(box.Options{
		Context: ctx,
		Options: options,
	})
	if err != nil {
		cancel()
		return err
	}
	if startGeph {
		err = checkStartGephEndpoints(instance)
	}
	closeErr := instance.Close()
	cancel()
	if err == nil {
		return closeErr
	}
	if closeErr != nil {
		err = exceptions.Append(err, closeErr, func(err error) error {
			return exceptions.Cause(err, "close configuration")
		})
	}
	return err
}

func checkStartGephEndpoints(instance *box.Box) error {
	var gephEndpoints []adapter.Endpoint
	for _, endpoint := range instance.Endpoint().Endpoints() {
		if endpoint.Type() != C.TypeGeph {
			continue
		}
		gephEndpoints = append(gephEndpoints, endpoint)
	}
	var started []adapter.Endpoint
	for _, endpoint := range gephEndpoints {
		started = append(started, endpoint)
		if err := endpoint.Start(adapter.StartStateStart); err != nil {
			return closeGephEndpoints(started, err)
		}
	}
	return closeGephEndpoints(started, nil)
}

func closeGephEndpoints(endpoints []adapter.Endpoint, err error) error {
	for i := len(endpoints) - 1; i >= 0; i-- {
		err = exceptions.Append(err, endpoints[i].Close(), func(closeErr error) error {
			return exceptions.Cause(closeErr, "close ", endpoints[i].Type(), "[", endpoints[i].Tag(), "]")
		})
	}
	return err
}
