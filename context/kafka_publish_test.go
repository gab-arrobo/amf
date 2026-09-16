// Copyright (c) 2026 Intel Corporation
// SPDX-License-Identifier: Apache-2.0

package context

import (
	"testing"

	"github.com/omec-project/amf/factory"
	"github.com/omec-project/openapi/v2/models"
	"github.com/omec-project/util/fsm"
	mi "github.com/omec-project/util/metricinfo"
)

// disableKafkaForTest mirrors the helper of the same shape in the gmm and ngap test packages:
// PublishUeCtxtInfo dereferences factory.AmfConfig.Configuration.KafkaInfo.EnableKafka, which is
// nil in a bare test.
func disableKafkaForTest(t *testing.T) {
	t.Helper()

	originalConfig := factory.AmfConfig.Configuration
	if originalConfig == nil {
		factory.AmfConfig.Configuration = &factory.Configuration{}
	}
	originalEnableKafka := factory.AmfConfig.Configuration.KafkaInfo.EnableKafka
	disabled := false
	factory.AmfConfig.Configuration.KafkaInfo.EnableKafka = &disabled
	t.Cleanup(func() {
		if originalConfig == nil {
			factory.AmfConfig.Configuration = nil
			return
		}
		factory.AmfConfig.Configuration = originalConfig
		factory.AmfConfig.Configuration.KafkaInfo.EnableKafka = originalEnableKafka
	})
}

// TestGetPublishUeCtxtInfoOp pins the state -> Kafka op mapping that the initial Add / later Mod /
// Del ordering depends on: Authentication must map to Add, everything past it to Mod, and the
// Deregistered states to Del.
func TestGetPublishUeCtxtInfoOp(t *testing.T) {
	tests := []struct {
		state fsm.StateType
		want  mi.SubscriberOp
	}{
		{Deregistered, mi.SubsOpDel},
		{DeregistrationInitiated, mi.SubsOpDel},
		{Authentication, mi.SubsOpAdd},
		{SecurityMode, mi.SubsOpMod},
		{ContextSetup, mi.SubsOpMod},
		{Registered, mi.SubsOpMod},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			if got := getPublishUeCtxtInfoOp(tt.state); got != tt.want {
				t.Errorf("getPublishUeCtxtInfoOp(%s) = %v, want %v", tt.state, got, tt.want)
			}
		})
	}
}

// TestBuildKafkaSubscriberContextUsesGivenAccessType guards the non-3GPP lifecycle fix: the op and
// AmfSubState must come from the access type that was passed in, not always from the 3GPP FSM
// instance. Both accesses are kept in non-Del states here so the dual-access Del/Mod downgrade
// (see TestBuildKafkaSubscriberContextKeepsSubscriberWhileOtherAccessActive) doesn't interfere.
func TestBuildKafkaSubscriberContextUsesGivenAccessType(t *testing.T) {
	ue := &AmfUe{}
	ue.init()
	ue.SetSupi("imsi-001010000000001")

	ue.State[models.ACCESSTYPE__3_GPP_ACCESS].Set(Registered)
	ue.State[models.ACCESSTYPE_NON_3_GPP_ACCESS].Set(SecurityMode)

	kafkaCtxt, op := ue.buildKafkaSubscriberContext(models.ACCESSTYPE_NON_3_GPP_ACCESS)
	if op != mi.SubsOpMod {
		t.Errorf("op for non-3GPP access = %v, want %v", op, mi.SubsOpMod)
	}
	if kafkaCtxt.AmfSubState != string(SecurityMode) {
		t.Errorf("AmfSubState = %q, want %q", kafkaCtxt.AmfSubState, string(SecurityMode))
	}

	// The 3GPP access type must still resolve independently to its own (Registered) state.
	kafkaCtxt3gpp, op3gpp := ue.buildKafkaSubscriberContext(models.ACCESSTYPE__3_GPP_ACCESS)
	if op3gpp != mi.SubsOpMod {
		t.Errorf("op for 3GPP access = %v, want %v", op3gpp, mi.SubsOpMod)
	}
	if kafkaCtxt3gpp.AmfSubState != string(Registered) {
		t.Errorf("AmfSubState = %q, want %q", kafkaCtxt3gpp.AmfSubState, string(Registered))
	}
}

// TestBuildKafkaSubscriberContextKeepsSubscriberWhileOtherAccessActive guards the dual-access fix
// (PR #825 Copilot review): the event is keyed only by IMSI with no access discriminator, so
// deregistering one access type must not delete the subscriber from Kafka consumers while the
// other access type is still registered/registering -- it must downgrade to Mod, and only emit
// Del once both access types have left the registered states. The payload must also come from the
// still-active access, not the deregistering one, or the Mod would overwrite the live subscriber
// record with the deregistering access's stale state and RAN identifiers.
func TestBuildKafkaSubscriberContextKeepsSubscriberWhileOtherAccessActive(t *testing.T) {
	ue := &AmfUe{}
	ue.init()
	ue.SetSupi("imsi-001010000000001")

	// 3GPP stays Registered while non-3GPP deregisters.
	ue.State[models.ACCESSTYPE__3_GPP_ACCESS].Set(Registered)
	ue.State[models.ACCESSTYPE_NON_3_GPP_ACCESS].Set(DeregistrationInitiated)

	kafkaCtxt, op := ue.buildKafkaSubscriberContext(models.ACCESSTYPE_NON_3_GPP_ACCESS)
	if op != mi.SubsOpMod {
		t.Errorf("op = %v, want %v (must not delete subscriber while 3GPP access is still active)", op, mi.SubsOpMod)
	}
	if kafkaCtxt.AmfSubState != string(Registered) {
		t.Errorf("AmfSubState = %q, want %q (must reflect surviving 3GPP access, not deregistering non-3GPP)",
			kafkaCtxt.AmfSubState, string(Registered))
	}

	// Once the 3GPP access also deregisters, the Del must go through.
	ue.State[models.ACCESSTYPE__3_GPP_ACCESS].Set(Deregistered)
	if _, op := ue.buildKafkaSubscriberContext(models.ACCESSTYPE_NON_3_GPP_ACCESS); op != mi.SubsOpDel {
		t.Errorf("op = %v, want %v (both accesses deregistered)", op, mi.SubsOpDel)
	}
}

// TestPublishUeCtxtInfoSkipsWithoutResolvedSupiOrKafka pins the two early-return guards that keep
// PublishUeCtxtInfo from ever publishing a subscriber event keyed on an empty imsi, and from
// touching the (possibly unconfigured) Kafka writer when Kafka is disabled.
func TestPublishUeCtxtInfoSkipsWithoutResolvedSupiOrKafka(t *testing.T) {
	disableKafkaForTest(t)

	ue := &AmfUe{}
	ue.init()

	// Kafka disabled: must return immediately, regardless of SUPI.
	ue.SetSupi("imsi-001010000000001")
	ue.PublishUeCtxtInfo(models.ACCESSTYPE__3_GPP_ACCESS)

	// SUPI unresolved (SUCI-only UE): must return immediately, even if Kafka were enabled.
	enabled := true
	factory.AmfConfig.Configuration.KafkaInfo.EnableKafka = &enabled
	ue.SetSupi("")
	ue.PublishUeCtxtInfo(models.ACCESSTYPE__3_GPP_ACCESS)
	// Reaching here without a panic (there is no configured Kafka writer) is the assertion: both
	// guards returned before touching metrics.GetWriter().
}
