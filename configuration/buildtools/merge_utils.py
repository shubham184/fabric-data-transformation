from ctypes import Array

def update_simple_props(element:dict, target:dict, updatable_props : Array):
    for prop in updatable_props:
        if prop in element:
            target[prop] = element[prop]
    return target


def update_element_in_list(element:dict, list: Array, id_column:str, updatable_props : Array):
        for item in list:
            if item.get(id_column) == element.get(id_column):
                update_simple_props(element, item, updatable_props=updatable_props)
                return item
                

def remove_element_from_list(element:dict, list: Array, id_column:str):
    for item in list:
        if item.get(id_column) == element.get(id_column):
            list.remove(item)
            break

def add_element_to_list(element:dict, list: Array, updatable_props: Array):
    new_item = {}
    update_simple_props(element, new_item, updatable_props=updatable_props)
    list.append(new_item)
    return new_item

def merge_data_to_list(element:dict, target_list: Array, id_column:str, other_props: Array):
    operation = element.get('operation','+')
    match operation:
        case '+':
            rslt = add_element_to_list(element, target_list, [id_column]+other_props)
            return rslt
        case '-':
            remove_element_from_list(element, target_list, id_column)
            return None
        case 'u'|'U':
            rslt = update_element_in_list(element, target_list,id_column, other_props)
            return rslt