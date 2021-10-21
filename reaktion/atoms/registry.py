import asyncio
from fluss.diagram import *
from reaktion.atoms.base import Atom
from typing import Dict, Type
from reaktion.atoms.reactive.combinators.zip import ZipAtom
from herre.wards.base import BaseWard


class NoAtomRegisted(Exception):
    pass



class AtomRegistry:

    def __init__(self) -> None:
        self.typeAtomClassMap: Dict[str, Type[Atom]] = {}

    def register_atom(self, type, atomClass: Type[Atom], overwrite=False):
        assert type not in self.typeAtomClassMap or overwrite, "We cannot register another Atom for this type. If you are sure specifiy the overwrite paramter"
        self.typeAtomClassMap[type] = atomClass

    def register_defaults(self):
        from .arkitekt import GenNoParallelArkitektAtom, FuncNoParallelArkitektAtom
        from .reactive import WithLatestFromAtom, CombineLatestAtom

        self.register_atom(ArkitektGenNode._type, GenNoParallelArkitektAtom)
        self.register_atom(ArkitektFuncNode._type, FuncNoParallelArkitektAtom)

        self.register_atom(WithLatestFromNode._type, WithLatestFromAtom) 
        self.register_atom(ZipNode._type, ZipAtom) 
        self.register_atom(CombineLatestNode._type, CombineLatestNode) 

    

    def get_atom(self, type) -> Atom:
        try:
            return self.typeAtomClassMap[type]
        except KeyError as e:
            raise NoAtomRegisted(f"No Structure registered for identifier {type}") from e




ATOM_REGISTRY = None

def get_atom_registry():
    global ATOM_REGISTRY
    if not ATOM_REGISTRY:
        ATOM_REGISTRY = AtomRegistry()
        ATOM_REGISTRY.register_defaults()


    return ATOM_REGISTRY