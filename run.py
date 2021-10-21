


import logging
from fakts.fakts import Fakts
from fakts.grants.yaml import YamlGrant
import herre
from reaktion.agent import FlussAgent
from rich.logging import RichHandler

logger = logging.getLogger()
logger.setLevel("INFO")

stream = RichHandler(markup=True)
logging.root.setLevel("INFO")
logging.root.addHandler(stream)


fakts = Fakts(name="fluss_test", grants=[YamlGrant(filepath="fluss.yaml")])

agent = FlussAgent(with_monitor=False, fakts=fakts)


agent.provide()