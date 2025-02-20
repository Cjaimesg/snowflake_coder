import json

def generate_step_descriptions(json_data):
    steps = json_data.get("steps", [])
    descriptions = []
    
    for step in steps:
        step_text = (f"Paso: {step['step_name']}\n"
                     f"Descripción: {step['long_step_description']}\n"
                     f"Objetivo: {step['objetive']}\n"
                     f"Objeto: {step['object']['name']} ({step['object']['type']})\n")
        descriptions.append(step_text)
    
    return descriptions