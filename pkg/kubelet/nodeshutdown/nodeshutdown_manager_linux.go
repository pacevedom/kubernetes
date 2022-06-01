//go:build linux
// +build linux

/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package nodeshutdown can watch for node level shutdown events and trigger graceful termination of pods running on the node prior to a system shutdown.
package nodeshutdown

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletevents "k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/eviction"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/nodeshutdown/systemd"
	"k8s.io/kubernetes/pkg/kubelet/prober"
	"k8s.io/utils/clock"
)

const (
	nodeShutdownReason             = "Terminated"
	nodeShutdownMessage            = "Pod was terminated in response to imminent node shutdown."
	nodeShutdownNotAdmittedReason  = "NodeShutdown"
	nodeShutdownNotAdmittedMessage = "Pod was rejected as the node is shutting down."
	dbusReconnectPeriod            = 1 * time.Second
	shutdownAllowedNamespaces      = "k8s.io/shutdownallowed"
)

var systemDbus = func() (dbusInhibiter, error) {
	return systemd.NewDBusCon()
}

type dbusInhibiter interface {
	CurrentInhibitDelay() (time.Duration, error)
	InhibitShutdown() (systemd.InhibitLock, error)
	BlockShutdown() (systemd.InhibitLock, error)
	ReleaseInhibitLock(lock systemd.InhibitLock) error
	ReloadLogindConf() error
	MonitorShutdown() (<-chan bool, error)
	OverrideInhibitDelay(inhibitDelayMax time.Duration) error
}

// managerImpl has functions that can be used to interact with the Node Shutdown Manager.
type managerImpl struct {
	recorder     record.EventRecorder
	nodeRef      *v1.ObjectReference
	probeManager prober.Manager

	shutdownGracePeriodByPodPriority []kubeletconfig.ShutdownGracePeriodByPodPriority

	getPods        eviction.ActivePodsFunc
	killPodFunc    eviction.KillPodFunc
	syncNodeStatus func()

	dbusCon     dbusInhibiter
	inhibitLock systemd.InhibitLock

	shutdownInhibitorAlertTimeLimit time.Duration
	shutdownInhibitorHolders        map[string]*time.Timer
	shutdownInhibitorHolderID       string
	shutdownInhibitorMutex          sync.Mutex

	nodeShuttingDownMutex sync.Mutex
	nodeShuttingDownNow   bool

	nodeLister corelisters.NodeLister

	clock           clock.Clock
	informerFactory informers.SharedInformerFactory
	leaseInformer   cache.SharedIndexInformer
}

// NewManager returns a new node shutdown manager.
func NewManager(conf *Config) (Manager, lifecycle.PodAdmitHandler) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.GracefulNodeShutdown) &&
		!utilfeature.DefaultFeatureGate.Enabled(features.ShutdownInhibitor) {
		m := managerStub{}
		return m, m
	}

	var shutdownGracePeriodByPodPriority []kubeletconfig.ShutdownGracePeriodByPodPriority

	if utilfeature.DefaultFeatureGate.Enabled(features.GracefulNodeShutdown) {
		shutdownGracePeriodByPodPriority = conf.ShutdownGracePeriodByPodPriority
		// Migration from the original configuration
		if !utilfeature.DefaultFeatureGate.Enabled(features.GracefulNodeShutdownBasedOnPodPriority) ||
			len(shutdownGracePeriodByPodPriority) == 0 {
			shutdownGracePeriodByPodPriority = migrateConfig(conf.ShutdownGracePeriodRequested, conf.ShutdownGracePeriodCriticalPods)
		}

		// Sort by priority from low to high
		sort.Slice(shutdownGracePeriodByPodPriority, func(i, j int) bool {
			return shutdownGracePeriodByPodPriority[i].Priority < shutdownGracePeriodByPodPriority[j].Priority
		})
	}

	var shutdownInhibitorAlertTimeLimit time.Duration
	var informerFactory informers.SharedInformerFactory
	var leaseInformer cache.SharedIndexInformer

	if utilfeature.DefaultFeatureGate.Enabled(features.ShutdownInhibitor) {
		shutdownInhibitorAlertTimeLimit = conf.ShutdownInhibitorAlertTimeout
		informerFactory = conf.InformerFactory
		leaseInformer = conf.InformerFactory.Coordination().V1().Leases().Informer()
	}

	if conf.Clock == nil {
		conf.Clock = clock.RealClock{}
	}
	manager := &managerImpl{
		probeManager:                     conf.ProbeManager,
		recorder:                         conf.Recorder,
		nodeRef:                          conf.NodeRef,
		getPods:                          conf.GetPodsFunc,
		killPodFunc:                      conf.KillPodFunc,
		syncNodeStatus:                   conf.SyncNodeStatusFunc,
		shutdownGracePeriodByPodPriority: shutdownGracePeriodByPodPriority,
		shutdownInhibitorAlertTimeLimit:  shutdownInhibitorAlertTimeLimit,
		shutdownInhibitorHolders:         make(map[string]*time.Timer),
		nodeLister:                       conf.NodeLister,
		clock:                            conf.Clock,
		informerFactory:                  informerFactory,
		leaseInformer:                    leaseInformer,
	}

	klog.InfoS("Creating node shutdown manager",
		"shutdownGracePeriodRequested", conf.ShutdownGracePeriodRequested,
		"shutdownGracePeriodCriticalPods", conf.ShutdownGracePeriodCriticalPods,
		"shutdownGracePeriodByPodPriority", shutdownGracePeriodByPodPriority,
		"shutdownInhibitorAlertTimeout", conf.ShutdownInhibitorAlertTimeout,
	)

	return manager, manager
}

// Admit rejects all pods if node is shutting
func (m *managerImpl) Admit(attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	nodeShuttingDown := m.ShutdownStatus() != nil

	if nodeShuttingDown {
		return lifecycle.PodAdmitResult{
			Admit:   false,
			Reason:  nodeShutdownNotAdmittedReason,
			Message: nodeShutdownNotAdmittedMessage,
		}
	}
	return lifecycle.PodAdmitResult{Admit: true}
}

// Start starts the node shutdown manager and will start watching the node for shutdown events.
func (m *managerImpl) Start() error {
	stop, err := m.start()
	if err != nil {
		return err
	}
	if m.shutdownGracePeriodByPodPriority == nil {
		return nil
	}
	go func() {
		for {
			if stop != nil {
				<-stop
			}

			time.Sleep(dbusReconnectPeriod)
			klog.V(1).InfoS("Restarting watch for node shutdown events")
			stop, err = m.start()
			if err != nil {
				klog.ErrorS(err, "Unable to watch the node for shutdown events")
			}
		}
	}()
	return nil
}

func (m *managerImpl) start() (chan struct{}, error) {
	if len(m.shutdownGracePeriodByPodPriority) == 0 && m.shutdownInhibitorAlertTimeLimit == 0 {
		return nil, nil
	}

	systemBus, err := systemDbus()
	if err != nil {
		return nil, err
	}
	m.dbusCon = systemBus

	if m.shutdownInhibitorAlertTimeLimit != 0 {
		m.leaseInformer.AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				AddFunc:    m.leaseAdd,
				UpdateFunc: m.leaseUpdate,
				DeleteFunc: m.leaseDelete,
			},
		)
		m.informerFactory.Start(wait.NeverStop)
	}

	if len(m.shutdownGracePeriodByPodPriority) == 0 {
		return nil, nil
	}

	currentInhibitDelay, err := m.dbusCon.CurrentInhibitDelay()
	if err != nil {
		return nil, err
	}

	// If the logind's InhibitDelayMaxUSec as configured in (logind.conf) is less than periodRequested, attempt to update the value to periodRequested.
	if periodRequested := m.periodRequested(); periodRequested > currentInhibitDelay {
		err := m.dbusCon.OverrideInhibitDelay(periodRequested)
		if err != nil {
			return nil, fmt.Errorf("unable to override inhibit delay by shutdown manager: %v", err)
		}

		err = m.dbusCon.ReloadLogindConf()
		if err != nil {
			return nil, err
		}

		// Read the current inhibitDelay again, if the override was successful, currentInhibitDelay will be equal to shutdownGracePeriodRequested.
		updatedInhibitDelay, err := m.dbusCon.CurrentInhibitDelay()
		if err != nil {
			return nil, err
		}

		if periodRequested > updatedInhibitDelay {
			return nil, fmt.Errorf("node shutdown manager was unable to update logind InhibitDelayMaxSec to %v (ShutdownGracePeriod), current value of InhibitDelayMaxSec (%v) is less than requested ShutdownGracePeriod", periodRequested, updatedInhibitDelay)
		}
	}

	err = m.aquireInhibitLock()
	if err != nil {
		return nil, err
	}

	events, err := m.dbusCon.MonitorShutdown()
	if err != nil {
		releaseErr := m.dbusCon.ReleaseInhibitLock(m.inhibitLock)
		if releaseErr != nil {
			return nil, fmt.Errorf("failed releasing inhibitLock: %v and failed monitoring shutdown: %v", releaseErr, err)
		}
		return nil, fmt.Errorf("failed to monitor shutdown: %v", err)
	}

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case isShuttingDown, ok := <-events:
				if !ok {
					klog.ErrorS(err, "Ended to watching the node for shutdown events")
					close(stop)
					return
				}
				klog.V(1).InfoS("Shutdown manager detected new shutdown event, isNodeShuttingDownNow", "event", isShuttingDown)

				var shutdownType string
				if isShuttingDown {
					shutdownType = "shutdown"
				} else {
					shutdownType = "cancelled"
				}
				klog.V(1).InfoS("Shutdown manager detected new shutdown event", "event", shutdownType)

				if isShuttingDown {
					m.recorder.Event(m.nodeRef, v1.EventTypeNormal, kubeletevents.NodeShutdown, "Shutdown manager detected shutdown event")
				} else {
					m.recorder.Event(m.nodeRef, v1.EventTypeNormal, kubeletevents.NodeShutdown, "Shutdown manager detected shutdown cancellation")
				}

				m.nodeShuttingDownMutex.Lock()
				m.nodeShuttingDownNow = isShuttingDown
				m.nodeShuttingDownMutex.Unlock()

				if isShuttingDown {
					// Update node status and ready condition
					go m.syncNodeStatus()
					m.processShutdownEvent()
				} else {
					m.aquireInhibitLock()
				}
			}
		}
	}()
	return stop, nil
}

func (m *managerImpl) aquireInhibitLock() error {
	lock, err := m.dbusCon.InhibitShutdown()
	if err != nil {
		return err
	}
	m.releaseInhibitLock()
	m.inhibitLock = lock
	return nil
}

func (m *managerImpl) acquireBlockLock() error {
	lock, err := m.dbusCon.BlockShutdown()
	if err != nil {
		return err
	}
	m.releaseInhibitLock()
	m.inhibitLock = lock
	return nil
}

func (m *managerImpl) releaseInhibitLock() {
	if m.inhibitLock != 0 {
		m.dbusCon.ReleaseInhibitLock(m.inhibitLock)
		m.inhibitLock = 0
	}
}

// ShutdownStatus will return an error if the node is currently shutting down.
func (m *managerImpl) ShutdownStatus() error {
	m.nodeShuttingDownMutex.Lock()
	defer m.nodeShuttingDownMutex.Unlock()

	if m.nodeShuttingDownNow {
		return fmt.Errorf("node is shutting down")
	}
	return nil
}

// ShutdownInhibited will return true if the node is currently unable to reboot
func (m *managerImpl) ShutdownInhibited() error {
	m.shutdownInhibitorMutex.Lock()
	defer m.shutdownInhibitorMutex.Unlock()
	if len(m.shutdownInhibitorHolders) > 0 {
		return fmt.Errorf("%s", m.shutdownInhibitorHolderID)
	}
	return nil
}

func (m *managerImpl) processShutdownEvent() error {
	klog.V(1).InfoS("Shutdown manager processing shutdown event")
	activePods := m.getPods()

	groups := groupByPriority(m.shutdownGracePeriodByPodPriority, activePods)
	for _, group := range groups {
		// If there are no pods in a particular range,
		// then do not wait for pods in that priority range.
		if len(group.Pods) == 0 {
			continue
		}

		var wg sync.WaitGroup
		wg.Add(len(group.Pods))
		for _, pod := range group.Pods {
			go func(pod *v1.Pod, group podShutdownGroup) {
				defer wg.Done()

				gracePeriodOverride := group.ShutdownGracePeriodSeconds

				// Stop probes for the pod
				m.probeManager.RemovePod(pod)

				// If the pod's spec specifies a termination gracePeriod which is less than the gracePeriodOverride calculated, use the pod spec termination gracePeriod.
				if pod.Spec.TerminationGracePeriodSeconds != nil && *pod.Spec.TerminationGracePeriodSeconds <= gracePeriodOverride {
					gracePeriodOverride = *pod.Spec.TerminationGracePeriodSeconds
				}

				klog.V(1).InfoS("Shutdown manager killing pod with gracePeriod", "pod", klog.KObj(pod), "gracePeriod", gracePeriodOverride)

				if err := m.killPodFunc(pod, false, &gracePeriodOverride, func(status *v1.PodStatus) {
					// set the pod status to failed (unless it was already in a successful terminal phase)
					if status.Phase != v1.PodSucceeded {
						status.Phase = v1.PodFailed
					}
					status.Message = nodeShutdownMessage
					status.Reason = nodeShutdownReason
				}); err != nil {
					klog.V(1).InfoS("Shutdown manager failed killing pod", "pod", klog.KObj(pod), "err", err)
				} else {
					klog.V(1).InfoS("Shutdown manager finished killing pod", "pod", klog.KObj(pod))
				}
			}(pod, group)
		}

		c := make(chan struct{})
		go func() {
			defer close(c)
			wg.Wait()
		}()

		select {
		case <-c:
		case <-time.After(time.Duration(group.ShutdownGracePeriodSeconds) * time.Second):
			klog.V(1).InfoS("Shutdown manager pod killing time out", "gracePeriod", group.ShutdownGracePeriodSeconds, "priority", group.Priority)
		}
	}

	m.dbusCon.ReleaseInhibitLock(m.inhibitLock)
	klog.V(1).InfoS("Shutdown manager completed processing shutdown event, node will shutdown shortly")

	return nil
}

func (m *managerImpl) periodRequested() time.Duration {
	var sum int64
	for _, period := range m.shutdownGracePeriodByPodPriority {
		sum += period.ShutdownGracePeriodSeconds
	}
	return time.Duration(sum) * time.Second
}

func (m *managerImpl) leaseAdd(obj interface{}) {
	if m.ShutdownStatus() != nil {
		klog.Info("shutdown inhibitor: ignored, node is shutting down")
		return
	}
	newLease := obj.(*coordinationv1.Lease)
	if newLease.Spec.HolderIdentity == nil || *newLease.Spec.HolderIdentity == "" ||
		newLease.Spec.AcquireTime == nil || newLease.Spec.AcquireTime.IsZero() ||
		newLease.Name != m.nodeRef.Name || newLease.Namespace == v1.NamespaceNodeLease ||
		!m.isNamespaceAllowed(newLease.Namespace) {
		return
	}
	m.addBlockHolder(holderIdentityUniqueName(newLease.Namespace, *newLease.Spec.HolderIdentity))
}

func (m *managerImpl) leaseUpdate(old, new interface{}) {
	if m.ShutdownStatus() != nil {
		klog.Info("shutdown inhibitor: ignored, node is shutting down")
		return
	}

	oldLease := old.(*coordinationv1.Lease)
	newLease := new.(*coordinationv1.Lease)
	if newLease.Name != m.nodeRef.Name || newLease.Namespace == v1.NamespaceNodeLease {
		return
	}
	if newLease.Spec.HolderIdentity != nil && oldLease.Spec.HolderIdentity != nil {
		if *newLease.Spec.HolderIdentity == "" && *newLease.Spec.HolderIdentity != *oldLease.Spec.HolderIdentity {
			m.removeBlockHolder(holderIdentityUniqueName(oldLease.Namespace, *oldLease.Spec.HolderIdentity))
		} else {
			if !m.isNamespaceAllowed(newLease.Namespace) {
				return
			}
			m.addBlockHolder(holderIdentityUniqueName(newLease.Namespace, *newLease.Spec.HolderIdentity))
		}
	}
}

func (m *managerImpl) leaseDelete(obj interface{}) {
	deletedLease := obj.(*coordinationv1.Lease)
	if deletedLease.Spec.HolderIdentity == nil || *deletedLease.Spec.HolderIdentity == "" ||
		deletedLease.Name != m.nodeRef.Name || deletedLease.Namespace == v1.NamespaceNodeLease {
		return
	}
	m.removeBlockHolder(holderIdentityUniqueName(deletedLease.Namespace, *deletedLease.Spec.HolderIdentity))
}

func (m *managerImpl) addBlockHolder(id string) {
	m.shutdownInhibitorMutex.Lock()
	defer m.shutdownInhibitorMutex.Unlock()

	if len(m.shutdownInhibitorHolders) == 0 {
		if err := m.acquireBlockLock(); err != nil {
			klog.ErrorS(err, "unable to create shutdown block inhibitor", "holderIdentity", id)
			return
		}
		m.shutdownInhibitorHolders[id] = time.AfterFunc(m.shutdownInhibitorAlertTimeLimit, func() {
			klog.InfoS("shutdown inhibitor: lease been held for too long", "holder", id)
			m.recorder.Event(m.nodeRef, v1.EventTypeWarning, kubeletevents.NodeShutdownInhibitor, fmt.Sprintf("Shutdown lease held for too long: %s", id))
		})
		m.shutdownInhibitorHolderID = id
		m.recorder.Event(m.nodeRef, v1.EventTypeNormal, kubeletevents.NodeShutdownInhibitor, "Shutdown inhibitor has been created")
		klog.InfoS("shutdown inhibitor: block acquired", "holder", m.shutdownInhibitorHolderID)
	} else {
		if _, ok := m.shutdownInhibitorHolders[id]; !ok {
			m.shutdownInhibitorHolders[id] = nil
			klog.InfoS("shutdown inhibitor: lease added to waiting list", "holder", id)
		}
	}
}

func (m *managerImpl) removeBlockHolder(id string) {
	m.shutdownInhibitorMutex.Lock()
	defer m.shutdownInhibitorMutex.Unlock()
	if len(m.shutdownInhibitorHolders) == 0 {
		return
	}

	if t, ok := m.shutdownInhibitorHolders[id]; ok && t != nil {
		t.Stop()
	}
	delete(m.shutdownInhibitorHolders, id)
	if len(m.shutdownInhibitorHolders) > 0 {
		nextID := ""
		for h := range m.shutdownInhibitorHolders {
			nextID = h
			break
		}
		m.shutdownInhibitorHolderID = nextID
		m.shutdownInhibitorHolders[nextID] = time.AfterFunc(m.shutdownInhibitorAlertTimeLimit, func() {
			klog.InfoS("shutdown inhibitor: lease been held for too long", "holder", nextID)
			m.recorder.Event(m.nodeRef, v1.EventTypeWarning, kubeletevents.NodeShutdownInhibitor, fmt.Sprintf("Shutdown lease held for too long: %s", nextID))
		})
		m.recorder.Event(m.nodeRef, v1.EventTypeNormal, kubeletevents.NodeShutdownInhibitor, fmt.Sprintf("Shutdown inhibitor new owner: %s", m.shutdownInhibitorHolderID))
		klog.InfoS("shutdown inhibitor: lease new owner", "holder", m.shutdownInhibitorHolderID)
	} else {
		m.shutdownInhibitorHolderID = ""
		if len(m.shutdownGracePeriodByPodPriority) > 0 {
			if err := m.aquireInhibitLock(); err != nil {
				klog.ErrorS(err, "unable to acquire shutdown delay inhibitor")
				return
			}
		} else {
			m.releaseInhibitLock()
		}
		m.recorder.Event(m.nodeRef, v1.EventTypeNormal, kubeletevents.NodeShutdownInhibitor, "Shutdown inhibitor removed")
		klog.InfoS("shutdown inhibitor: all leases released, block removed")
	}
}

func (m *managerImpl) isNamespaceAllowed(namespace string) bool {
	node, err := m.nodeLister.Get(m.nodeRef.Name)
	if err != nil {
		klog.ErrorS(err, "unable to get node", "node", m.nodeRef.Name)
		return false
	}
	if allowedNamespaces, ok := node.Annotations[shutdownAllowedNamespaces]; ok {
		if strings.Contains(allowedNamespaces, namespace) {
			return true
		}
	}
	return false
}

func migrateConfig(shutdownGracePeriodRequested, shutdownGracePeriodCriticalPods time.Duration) []kubeletconfig.ShutdownGracePeriodByPodPriority {
	if shutdownGracePeriodRequested == 0 {
		return nil
	}
	defaultPriority := shutdownGracePeriodRequested - shutdownGracePeriodCriticalPods
	if defaultPriority < 0 {
		return nil
	}
	criticalPriority := shutdownGracePeriodRequested - defaultPriority
	if criticalPriority < 0 {
		return nil
	}
	return []kubeletconfig.ShutdownGracePeriodByPodPriority{
		{
			Priority:                   scheduling.DefaultPriorityWhenNoDefaultClassExists,
			ShutdownGracePeriodSeconds: int64(defaultPriority / time.Second),
		},
		{
			Priority:                   scheduling.SystemCriticalPriority,
			ShutdownGracePeriodSeconds: int64(criticalPriority / time.Second),
		},
	}
}

func groupByPriority(shutdownGracePeriodByPodPriority []kubeletconfig.ShutdownGracePeriodByPodPriority, pods []*v1.Pod) []podShutdownGroup {
	groups := make([]podShutdownGroup, 0, len(shutdownGracePeriodByPodPriority))
	for _, period := range shutdownGracePeriodByPodPriority {
		groups = append(groups, podShutdownGroup{
			ShutdownGracePeriodByPodPriority: period,
		})
	}

	for _, pod := range pods {
		var priority int32
		if pod.Spec.Priority != nil {
			priority = *pod.Spec.Priority
		}

		// Find the group index according to the priority.
		index := sort.Search(len(groups), func(i int) bool {
			return groups[i].Priority >= priority
		})

		// 1. Those higher than the highest priority default to the highest priority
		// 2. Those lower than the lowest priority default to the lowest priority
		// 3. Those boundary priority default to the lower priority
		// if priority of pod is:
		//   groups[index-1].Priority <= pod priority < groups[index].Priority
		// in which case we want to pick lower one (i.e index-1)
		if index == len(groups) {
			index = len(groups) - 1
		} else if index < 0 {
			index = 0
		} else if index > 0 && groups[index].Priority > priority {
			index--
		}

		groups[index].Pods = append(groups[index].Pods, pod)
	}
	return groups
}

type podShutdownGroup struct {
	kubeletconfig.ShutdownGracePeriodByPodPriority
	Pods []*v1.Pod
}

func holderIdentityUniqueName(namespace, holder string) string {
	return fmt.Sprintf("%s/%s", namespace, holder)
}
