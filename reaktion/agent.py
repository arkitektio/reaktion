from asyncio.tasks import create_task
from reaktion.actor import ReactiveGraphActor
from arkitekt.agents.stateful import StatefulAgent
import logging

logger = logging.getLogger(__name__)


class ReaktionAgent(StatefulAgent):
    async def on_bounced_provide(self, message):
        actor = ReactiveGraphActor()
