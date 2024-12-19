from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass
import networkx as nx

@dataclass
class Entity:
    id: str
    type: str
    attributes: Dict[str, Any]

@dataclass
class Relation:
    source: str
    target: str
    type: str
    weight: float

class KnowledgeGraphManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.graph = nx.MultiDiGraph()
        self.entities: Dict[str, Entity] = {}
        self.relation_types: Set[str] = set()
        
    async def add_entity(self, entity: Entity) -> bool:
        """Add new entity to knowledge graph"""
        if entity.id not in self.entities:
            self.entities[entity.id] = entity
            self.graph.add_node(entity.id, **entity.attributes)
            return True
        return False
    
    async def add_relation(self, relation: Relation) -> bool:
        """Add new relation to knowledge graph"""
        if relation.source in self.entities and relation.target in self.entities:
            self.graph.add_edge(
                relation.source,
                relation.target,
                type=relation.type,
                weight=relation.weight
            )
            self.relation_types.add(relation.type)
            return True
        return False
    
    async def query_subgraph(self, 
                            root_entity: str, 
                            max_depth: int = 2) -> nx.MultiDiGraph:
        """Query subgraph starting from root entity"""
        if root_entity not in self.entities:
            return nx.MultiDiGraph()
        
        subgraph_nodes = set([root_entity])
        current_depth = 0
        frontier = {root_entity}
        
        while current_depth < max_depth and frontier:
            next_frontier = set()
            for node in frontier:
                neighbors = set(self.graph.neighbors(node))
                subgraph_nodes.update(neighbors)
                next_frontier.update(neighbors)
            frontier = next_frontier
            current_depth += 1
            
        return self.graph.subgraph(subgraph_nodes)
    
    async def find_path(self, 
                       source: str, 
                       target: str, 
                       relation_type: Optional[str] = None) -> List[str]:
        """Find path between two entities"""
        if source not in self.entities or target not in self.entities:
            return []
            
        if relation_type:
            # Filter edges by relation type
            def filter_edges(u, v, data):
                return data.get('type') == relation_type
            try:
                path = nx.shortest_path(
                    self.graph,
                    source=source,
                    target=target,
                    weight='weight'
                )
                return path
            except nx.NetworkXNoPath:
                return []
        else:
            try:
                return nx.shortest_path(
                    self.graph,
                    source=source,
                    target=target,
                    weight='weight'
                )
            except nx.NetworkXNoPath:
                return []
