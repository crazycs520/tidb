// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/printer"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	// ServerInformationPath store server information such as IP, port and so on.
	ServerInformationPath = "/tidb/server/info"
)

// InfoSyncer stores server info to Etcd when when the tidb-server starts and delete when tidb-server shuts down.
type InfoSyncer struct {
	etcdCli        *clientv3.Client
	info           *ServerInfo
	serverInfoPath string
}

// ServerInfo is server static information.
// It will not update when server running. So please only put static information in ServerInfo struct.
type ServerInfo struct {
	ServerVersionInfo
	ID         string `json:"ddl_id"`
	IP         string `json:"ip"`
	Port       uint   `json:"listening_port"`
	StatusPort uint   `json:"status_port"`
	Lease      string `json:"lease"`
}

// ServerVersionInfo is the server version and git_hash.
type ServerVersionInfo struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
}

// NewInfoSyncer return new InfoSyncer. It is exported for testing.
func NewInfoSyncer(id string, etcdCli *clientv3.Client) *InfoSyncer {
	return &InfoSyncer{
		etcdCli:        etcdCli,
		info:           getServerInfo(id),
		serverInfoPath: fmt.Sprintf("%s/%s", ServerInformationPath, id),
	}
}

func getServerInfo(id string) *ServerInfo {
	cfg := config.GetGlobalConfig()
	info := &ServerInfo{
		ID:         id,
		IP:         cfg.AdvertiseAddress,
		Port:       cfg.Port,
		StatusPort: cfg.Status.StatusPort,
		Lease:      cfg.Lease,
	}
	info.Version = mysql.ServerVersion
	info.GitHash = printer.TiDBGitHash
	return info
}

//GetServerInfo gets self server static information.
func (is *InfoSyncer) GetServerInfo() *ServerInfo {
	return is.info
}

// GetServerInfoByID gets owner server static information from Etcd.
func (is *InfoSyncer) GetServerInfoByID(ctx context.Context, id string) (*ServerInfo, error) {
	if is.etcdCli == nil || id == is.info.ID {
		return is.info, nil
	}
	key := fmt.Sprintf("%s/%s", ServerInformationPath, id)
	infoMap, err := getInfo(ctx, is.etcdCli, key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	info, ok := infoMap[id]
	if !ok {
		return nil, errors.Errorf("[infoSyncer] get %s failed", key)
	}
	return info, nil
}

// GetAllServerInfo gets all servers static information from Etcd.
func (is *InfoSyncer) GetAllServerInfo(ctx context.Context) (map[string]*ServerInfo, error) {
	allInfo := make(map[string]*ServerInfo)
	if is.etcdCli == nil {
		allInfo[is.info.ID] = is.info
		return allInfo, nil
	}
	allInfo, err := getInfo(ctx, is.etcdCli, ServerInformationPath, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return allInfo, nil
}

// StoreServerInfo stores self server static information to Etcd when domain Init.
func (is *InfoSyncer) StoreServerInfo(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	infoBuf, err := json.Marshal(is.info)
	if err != nil {
		return errors.Trace(err)
	}
	err = ddl.PutKVToEtcd(ctx, is.etcdCli, ddl.KeyOpDefaultRetryCnt, is.serverInfoPath, hack.String(infoBuf))
	return errors.Trace(err)
}

// RemoveServerInfo remove self server static information from Etcd.
func (is *InfoSyncer) RemoveServerInfo() {
	if is.etcdCli == nil {
		return
	}
	err := ddl.DeleteKeyFromEtcd(is.serverInfoPath, is.etcdCli)
	if err != nil {
		log.Errorf("[infoSyncer] remove self server info failed %v", err)
	}
}

// getInfo gets server information from Etcd according to the key and opts.
func getInfo(ctx context.Context, etcdCli *clientv3.Client, key string, opts ...clientv3.OpOption) (map[string]*ServerInfo, error) {
	var err error
	allInfo := make(map[string]*ServerInfo)
	for {
		select {
		case <-ctx.Done():
			err = errors.Trace(ctx.Err())
			return nil, err
		default:
		}
		childCtx, cancel := context.WithTimeout(ctx, ddl.KeyOpDefaultTimeout)
		resp, err := etcdCli.Get(childCtx, key, opts...)
		cancel()
		if err != nil {
			log.Infof("[infoSyncer] get %s failed %v, continue checking.", key, err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, kv := range resp.Kvs {
			info := &ServerInfo{}
			err := json.Unmarshal(kv.Value, info)
			if err != nil {
				log.Infof("[infoSyncer] get %s, json.Unmarshal %v failed %v.", kv.Key, kv.Value, err)
				return nil, errors.Trace(err)
			}
			allInfo[info.ID] = info
		}
		return allInfo, nil
	}
}
