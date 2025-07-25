import glob
import json
import yaml
import json5
from pathlib import Path
from typing import Any, Dict

from b2s import B2S_Definition
from dag_util import generate_dag_html, sort_elements_list

def get_definition_names(folders, extensions):
    files = []
    for fldr in folders:
        for ext in extensions:
            files += glob.glob(f"{fldr}/*.{ext}")
    names = list(set([(Path(filepath).name).split('.')[0] for filepath in files]))
    return names

def get_files_for_name(folders, extensions, name:str) -> list[Path]:
    files = []
    for fldr in folders:
        for ext in extensions:
            files += glob.glob(f"{fldr}/{name}.*{ext}")
    return [Path(f) for f in files]

def load_data_from_path(path: Path) -> Dict[str, Any]:
    """
    Load a JSON, YAML, or JSON5 file from the given path and return a dictionary.
    """
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"The path {path} does not exist or is not a file.")
    extension = path.suffix.lower()
    with path.open('r', encoding='utf-8') as file:
        if extension == '.json':
            data = json.load(file)
        elif extension in {'.yaml', '.yml'}:
            data = yaml.safe_load(file)
        elif extension == '.json5':
            data = json5.load(file)
        else:
            raise ValueError(f"Unsupported file extension: {extension}")
    if not isinstance(data, dict):
        raise TypeError(f"Expected top-level object to be a dictionary, got {type(data).__name__}")
    return data


if __name__ == "__main__":
    b2s_folders = ["0_product/B2S","1_industry/B2S","2_Customer/B2S"]
    extensions = ["json", "yaml","json5"]
    
    b2s_elements = []
    names = get_definition_names(folders=b2s_folders, extensions=extensions)
    for name in names:
        element = B2S_Definition(name)
        print(f'Element: B2S/{name}:')
        for file in get_files_for_name(folders=b2s_folders, extensions=extensions, name=name):
            data = load_data_from_path(file)
            element.add_filedata(data)
        output_path = Path('.build/B2S')
        output_path.mkdir(parents=True, exist_ok=True)
        print('  + WRITING MERGED JSON')
        element.save_to_json(output_path/f'{name}.json')
        print('  + WRITING RENDERED OUTPUT')
        element.save_to_sql(output_path/f'{name}.sql')
        b2s_elements.append(element)
    sorted_b2ds = sort_elements_list(b2s_elements)
    for n in sorted_b2ds:
        print(f" {n[1]} - {n[0].get_id()}")
    dag_elements = [{"id":e.target, "predecessors": e.predecessors} for e in b2s_elements]
    generate_dag_html(dag_elements, output_file=Path(".build/b2s_dag.html")) 