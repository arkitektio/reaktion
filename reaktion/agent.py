from asyncio.tasks import create_task
from reaktion.actor import ReactiveGraphActor
from re import template
from fluss.schema import Graph
from arkitekt.messages.postman.provide.provide_transition import (
    ProvideState,
    ProvideTransitionMessage,
)

from arkitekt.messages.postman.provide.bounced_provide import BouncedProvideMessage

from arkitekt.agents.base import AgentException
import logging
from arkitekt.agents.app import AppAgent

logger = logging.getLogger(__name__)


class ReaktionAgent(AppAgent):
    ACTOR_PENDING_MESSAGE = "Actor is Pending"

    def __init__(self, *args, strict=False, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.strict = strict

    async def on_bounced_provide(self, message: BouncedProvideMessage):
        try:
            if message.meta.reference in self.runningActors:
                if self.strict:
                    raise AgentException(
                        "Already Running Provision Received Again. Right now causing Error. Might be omitted"
                    )
                again_provided = ProvideTransitionMessage(
                    data={
                        "message": "Provision was running on this Instance. Probably a freaking race condition",
                        "state": ProvideState.ACTIVE,
                    },
                    meta={
                        "extensions": message.meta.extensions,
                        "reference": message.meta.reference,
                    },
                )
                await self.transport.forward(again_provided)
            else:
                actor = ReactiveGraphActor()
                logger.info("Created new Reactive Graph Actor")
                self.runningActors[message.meta.reference] = actor
                task = create_task(actor.arun(message, self))
                task.add_done_callback(self.on_task_done)
                self.runningTasks[message.meta.reference] = task

        except Exception as e:
            raise AgentException("No approved actors for this template") from e
