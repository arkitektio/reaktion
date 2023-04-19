import pytest
from fluss.api.schema import (
    FlowFragment,
    ArkitektNodeFragment,
    RunMutationStart,
    SnapshotMutationSnapshot,
    TrackMutationTrack,
)
from rekuest.messages import Provision, Assignation
from rekuest.agents.transport.protocols.agent_json import *
from reaktion.actor import FlowActor
from rekuest.agents.transport.mock import MockAgentTransport
from rekuest.postmans.utils import mockuse


async def retrieve_chunk_contractor(node: ArkitektNodeFragment, provision: Provision):
    """This function mocks the contractor for the add_three_flow."""

    return mockuse(
        returns=[
            streamitem.mock(list_len_generator=lambda: 3)
            for streamitem in node.outstream[0]
        ],
        reserve_sleep=0.1,
        assign_sleep=0.1,
        stream_sleep=0.1,
    )


@pytest.mark.asyncio
@pytest.mark.actor
async def test_provide_actor(retrieve_chunk_flow: FlowFragment):

    provision = Provision(provision=1, guardian=1, user=1)
    Assignation(assignation=1, user=1, provision=1, args=[])

    tracki = 0
    runi = 0
    snapshoti = 0

    async def atrackrun(source, handle, type, value):
        nonlocal tracki
        tracki += 1
        return TrackMutationTrack(
            id=tracki, source=source, handle=handle, type=type, value=value
        )

    async def amockrun(assignation=None, flow=None):
        nonlocal runi
        runi += 1
        return RunMutationStart(id=runi)

    async def amocksnapshot(run=None, events=None, t=None):
        nonlocal snapshoti
        snapshoti += 1
        return SnapshotMutationSnapshot(id=snapshoti, run=run, events=events, t=t)

    async with MockAgentTransport() as transport:

        async with FlowActor(
            provision=provision,
            transport=transport,
            flow=retrieve_chunk_flow,
            is_generator=True,
            nodeContractor=retrieve_chunk_contractor,
            run_mutation=amockrun,
            snapshot_mutation=amocksnapshot,
            track_mutation=atrackrun,
        ) as actor:

            await actor.provide()
            x = await transport.areceive(timeout=1)
            assert isinstance(x, ProvisionChangedMessage)
            assert x.status == ProvisionStatus.ACTIVE

            for i in actor.contracts.values():
                assert i.active is True


@pytest.mark.asyncio
@pytest.mark.actor
async def test_provide_assign(retrieve_chunk_flow: FlowFragment):

    provision = Provision(provision=1, guardian=1, user=1)
    assignation = Assignation(assignation=1, user=1, provision=1, args=[2])

    tracki = 0
    runi = 0
    snapshoti = 0

    async def atrackrun(run, source, handle, type, value, t):
        nonlocal tracki
        tracki += 1
        return TrackMutationTrack(
            id=tracki,
            source=source,
            handle=handle,
            type=type,
            value=value,
        )

    async def amockrun(assignation=None, flow=None):
        nonlocal runi
        runi += 1
        return RunMutationStart(id=runi)

    async def amocksnapshot(run=None, events=None, t=None):
        nonlocal snapshoti
        snapshoti += 1
        return SnapshotMutationSnapshot(id=snapshoti, run=run, events=events, t=t)

    async with MockAgentTransport() as transport:

        async with FlowActor(
            provision=provision,
            transport=transport,
            flow=retrieve_chunk_flow,
            is_generator=True,
            nodeContractor=retrieve_chunk_contractor,
            run_mutation=amockrun,
            snapshot_mutation=amocksnapshot,
            track_mutation=atrackrun,
        ) as actor:

            await actor.provide()
            x = await transport.areceive(timeout=1)
            assert isinstance(x, ProvisionChangedMessage)
            assert x.status == ProvisionStatus.ACTIVE

            for i in actor.contracts.values():
                assert i.active is True

            await actor.process(assignation)

            for i in range(3):
                x = await transport.areceive(timeout=2)
                assert isinstance(x, AssignationChangedMessage)
                assert x.status == AssignationStatus.YIELD
                assert isinstance(x.returns[0], str)

            x = await transport.areceive(timeout=2)
            assert isinstance(x, AssignationChangedMessage)
            assert x.status == AssignationStatus.DONE
