import json


def generate_step_descriptions(json_data):
    steps = json_data.get("steps", [])
    descriptions = []

    for step in steps:
        step_text = (f"Step: {step['step_name']}\n"
                     f"Description: {step['long_step_description']}\n"
                     f"Objective: {step['objective']}\n"
                     f"Object: {step['object']['name']} ({step['object']['type']})\n"
                     f"Context: {step['context']}"
                     )
        descriptions.append(step_text)

    return descriptions
