import json


def generate_step_descriptions(json_data: dict):
    steps = json_data.get("steps", [])
    descriptions = []

    for step in steps:
        step_text = (f"## {step['step_name']}\n\n"
                     f"**Descripción:** {step['long_step_description']}\n\n"
                     f"**Objetivo:** {step['objective']}\n\n"
                     f"**Objeto:** {step['object']['name']} ({step['object']['type']})\n\n"
                     f"**Contexto:** {step['context']}\n")
        descriptions.append(step_text)

    return descriptions



def get_step_type(json_data: dict):
    steps = json_data.get("steps", [])
    types = []

    for step in steps:
        types.append(step['step_type'])

    return types
