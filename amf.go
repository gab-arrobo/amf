// SPDX-FileCopyrightText: 2024 Intel Corporation
// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"fmt"
	"os"

	"github.com/omec-project/amf/logger"
	"github.com/omec-project/amf/service"
	"github.com/urfave/cli"
)

var AMF = &service.AMF{}

func main() {
	app := cli.NewApp()
	app.Name = "amf"
	logger.AppLog.Infoln(app.Name)
	app.Usage = "-free5gccfg common configuration file -amfcfg amf configuration file"
	app.Action = action
	app.Flags = AMF.GetCliCmd()
	if err := app.Run(os.Args); err != nil {
		logger.AppLog.Errorf("AMF Run error: %v", err)
		return
	}
}

func action(c *cli.Context) error {
	if err := AMF.Initialize(c); err != nil {
		logger.CfgLog.Errorf("%+v", err)
		return fmt.Errorf("failed to initialize")
	}

	AMF.WatchConfig()

	AMF.Start()

	return nil
}
