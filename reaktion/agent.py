from reaktion.actor import ReactiveGraphActor
from re import template
from fluss.schema import Graph
from arkitekt.messages.postman.assign.assign_cancelled import AssignCancelledMessage
from arkitekt.messages.postman.unassign.bounced_forwarded_unassign import BouncedForwardedUnassignMessage
from arkitekt.messages.postman.provide.provide_transition import ProvideState, ProvideTransitionMessage
from arkitekt.messages.postman.assign.assign_log import AssignLogMessage
from arkitekt.messages.postman.assign.assign_critical import AssignCriticalMessage
from arkitekt.messages.postman.assign.bounced_forwarded_assign import BouncedForwardedAssignMessage
from arkitekt.messages.postman.assign.bounced_assign import BouncedAssignMessage
from arkitekt.messages.postman.provide.provide_critical import ProvideCriticalMessage
from arkitekt.messages.postman.log import LogLevel
from arkitekt.messages.postman.provide.provide_log import ProvideLogMessage
import asyncio
from herre.wards.base import WardException
from arkitekt.actors.actify import actify, define
from arkitekt.actors.base import Actor
from arkitekt.messages.postman.unprovide.bounced_unprovide import BouncedUnprovideMessage
from arkitekt.messages.base import MessageDataModel, MessageModel
from arkitekt.messages.postman.provide.bounced_provide import BouncedProvideMessage
from arkitekt.schema.template import Template
from arkitekt.schema.node import Node
from arkitekt.packers.transpilers import Transpiler
from typing import Callable, Dict, List, Tuple, Type
from arkitekt.agents.base import Agent, AgentException, parse_params
import logging
from arkitekt.agents.app import AppAgent
from herre.console import get_current_console

logger = logging.getLogger(__name__)


class FlussAgent(AppAgent):
    ACTOR_PENDING_MESSAGE = "Actor is Pending"

    def __init__(self, *args, strict=False, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.strict = strict

    async def on_bounced_provide(self, message: BouncedProvideMessage):
        try:
            if message.meta.reference in self.runningActors:
                if self.strict: raise AgentException("Already Running Provision Received Again. Right now causing Error. Might be omitted")
                again_provided = ProvideTransitionMessage(data={
                    "message": "Provision was running on this Instance. Probably a freaking race condition",
                    "state": ProvideState.ACTIVE
                    }, meta={"extensions": message.meta.extensions, "reference": message.meta.reference})
                await self.transport.forward(again_provided)
            else:
                actor = ReactiveGraphActor()
                logger.info("Created new Reactive Graph Actor")
                self.runningActors[message.meta.reference] = actor
                task = self.loop.create_task(actor.arun(message, self))
                task.add_done_callback(self.on_task_done)
                self.runningTasks[message.meta.reference] = task
        
        except Exception as e:
            raise AgentException("No approved actors for this template") from e






