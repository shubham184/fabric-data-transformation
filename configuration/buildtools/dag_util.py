import abc
from typing import NamedTuple, Optional
from pyvis.network import Network
import networkx as nx

def generate_dag_html(elements, output_file="dag.html"):
    G = nx.DiGraph()
    
    for elem in elements:
        G.add_node(elem["id"])
        for pred in elem.get("predecessors", []):
            G.add_edge(pred, elem["id"])

    net = Network(directed=True)
    net.from_nx(G)
    net.set_options("""
     var options = {
        "configure": {
                "enabled": false
        },
        "layout" : {
            "hierarchical":  {
                "direction": "UD",  
                "sortMethod": "directed"
            }
        },     
  "edges": {
    "color": {
      "inherit": true
    },
    "smooth": false
  },
  "physics": {
    "barnesHut": {
      "gravitationalConstant": -12050
    },
    "minVelocity": 0.75
  }
}
    """)
    net.save_graph(output_file.__str__()) 
    print(f"Interactive DAG saved as {output_file}")

class DagEnabledElementInterface(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'get_id') and 
                callable(subclass.get_id) and 
                hasattr(subclass, 'get_predecessors') and 
                callable(subclass.get_predecessors) )

    @abc.abstractmethod
    def get_id(self)->str:
        """return id of the object"""
        raise NotImplementedError

    @abc.abstractmethod
    def extract_text(self)->list[str]:
        """return id list of all predecessors"""
        raise NotImplementedError

def sort_elements_list(lst:list[DagEnabledElementInterface]) -> Optional[list[tuple[DagEnabledElementInterface,int]]]:
    class ElementInfo(NamedTuple):
        object: DagEnabledElementInterface
        id:str
        predecessors:set[str]

    result:list[tuple[DagEnabledElementInterface,int]]=[]
    todo:list[ElementInfo]=[ElementInfo(e, e.get_id(), set(e.get_predecessors())) for e in lst]
    wave=0
    done_ids=set()
    while(todo and wave < len(lst)):
      wave_ids=set()
      for e in list(todo):
        if e.predecessors.issubset(done_ids):
          wave_ids.add(e.id)
          result.append((e.object, wave))
          todo.remove(e)
      done_ids.update(wave_ids)
      wave+=1
      if len(wave_ids)==0:
        break

    if todo:
      # not all elements have been processed > we have a circular reference so return None
      return None
    return result

if __name__ == "__main__":
    elements = [
    {"id": "A", "predecessors": []},
    {"id": "B", "predecessors": ["A"]},
    {"id": "C", "predecessors": ["A"]},
    {"id": "D", "predecessors": ["B", "C"]},
    {"id": "E", "predecessors": ["C"]},
    ]
    # Example usage
    generate_dag_html(elements)
