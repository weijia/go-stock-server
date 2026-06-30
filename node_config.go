// go-stock-server/node_config.go - 节点配置存储
package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"
)

// NodeConfig 节点配置
type NodeConfig map[string]interface{}

// NodeConfigStore 节点配置存储（内存 + 定时同步）
type NodeConfigStore struct {
	mu            sync.RWMutex
	configs       map[string]NodeConfig
	syncInterval  int // 秒
	stopCh        chan struct{}
}

// NewNodeConfigStore 创建节点配置存储
func NewNodeConfigStore(syncInterval int) *NodeConfigStore {
	store := &NodeConfigStore{
		configs:      make(map[string]NodeConfig),
		syncInterval: syncInterval,
		stopCh:       make(chan struct{}),
	}

	if syncInterval > 0 {
		go store.syncLoop()
	}

	return store
}

// Get 获取节点配置
func (s *NodeConfigStore) Get(nodeID string) NodeConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfg, ok := s.configs[nodeID]
	if !ok {
		return nil
	}
	// 返回副本
	cp := make(NodeConfig)
	for k, v := range cfg {
		cp[k] = v
	}
	return cp
}

// Create 创建默认节点配置
func (s *NodeConfigStore) Create(nodeID string) NodeConfig {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg := NodeConfig{
		"node_id":        nodeID,
		"created_at":     time.Now().Format(time.RFC3339),
		"updated_at":     time.Now().Format(time.RFC3339),
		"monitor_stocks": []string{},
		"settings": map[string]interface{}{
			"alert_threshold": 1.0,
			"check_interval":  5,
		},
	}
	s.configs[nodeID] = cfg
	return s.copyConfig(cfg)
}

// Update 更新节点配置
func (s *NodeConfigStore) Update(nodeID string, data map[string]interface{}) (NodeConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, ok := s.configs[nodeID]
	if !ok {
		cfg = NodeConfig{
			"node_id":    nodeID,
			"created_at": time.Now().Format(time.RFC3339),
		}
	}

	// 增量合并
	for k, v := range data {
		if k == "node_id" || k == "created_at" {
			continue
		}
		if existing, ok := cfg[k]; ok {
			existingMap, existingIsMap := existing.(map[string]interface{})
			newMap, newIsMap := v.(map[string]interface{})
			if existingIsMap && newIsMap {
				for nk, nv := range newMap {
					existingMap[nk] = nv
				}
				cfg[k] = existingMap
				continue
			}
		}
		cfg[k] = v
	}

	cfg["updated_at"] = time.Now().Format(time.RFC3339)
	s.configs[nodeID] = cfg

	return s.copyConfig(cfg), nil
}

// Delete 删除节点配置（重置为默认）
func (s *NodeConfigStore) Delete(nodeID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.configs[nodeID]; !ok {
		return false
	}

	// 重置为默认
	s.configs[nodeID] = NodeConfig{
		"node_id":        nodeID,
		"created_at":     time.Now().Format(time.RFC3339),
		"updated_at":     time.Now().Format(time.RFC3339),
		"monitor_stocks": []string{},
		"settings": map[string]interface{}{
			"alert_threshold": 1.0,
			"check_interval":  5,
		},
	}
	return true
}

// copyConfig 深拷贝配置
func (s *NodeConfigStore) copyConfig(cfg NodeConfig) NodeConfig {
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil
	}
	var cp NodeConfig
	json.Unmarshal(data, &cp)
	return cp
}

// syncLoop 定时同步循环
func (s *NodeConfigStore) syncLoop() {
	ticker := time.NewTicker(time.Duration(s.syncInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("[NodeConfig] 定时同步配置...")
			// 实际部署时可从文件或 GitHub 同步
			s.doSync()
		case <-s.stopCh:
			return
		}
	}
}

// doSync 执行同步
func (s *NodeConfigStore) doSync() {
	// 基础实现：简单打印同步状态
	s.mu.RLock()
	count := len(s.configs)
	s.mu.RUnlock()
	if count > 0 {
		log.Printf("[NodeConfig] 同步完成: %d 个节点配置", count)
	}
}
