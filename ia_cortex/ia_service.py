from jinja2 import Template


class BaseTextGenerator:
    def __init__(self,
                 session,
                 model: str = 'claude-3-5-sonnet',
                 prompt_template_file: str = None):
        """
        Inicializa el objeto BaseTextGenerator.

        Parámetros:
          - session: objeto de sesión de Snowflake.
          - cortex_search_service: instancia de CortexSearchService para generar el contexto.
          - model: nombre del modelo a utilizar en COMPLETE.
          - prompt_template_file: ruta del archivo de plantilla para el prompt.
        """
        self.session = session
        self.model = model

        if prompt_template_file:
            try:
                with open(prompt_template_file, 'r') as file:
                    prompt_template_text = file.read()
                    self.template = Template(prompt_template_text)
            except FileNotFoundError:
                raise ValueError(f"El archivo de plantilla '{prompt_template_file}' no fue encontrado.")
            except Exception as e:
                raise RuntimeError(f"Error al leer el archivo de plantilla: {e}")
        else:
            self.template = None

    def generate_response(self, params: dict):
        """
        Llama a la función COMPLETE de Snowflake para generar texto a partir del prompt.

        Parámetros:
          - params: diccionario con valores para la plantilla del prompt.

        Retorna:
          - La respuesta generada por el modelo de completado.
        """
        prompt = self.generate_prompt(params)

        self.query_cortex = "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS RESPONSE"

        result = self.session.sql(self.query_cortex, (self.model, prompt)).collect()[0]['RESPONSE']
        return result

    def generate_prompt(self, params: dict):
        """
        Genera un prompt a partir de un diccionario de parámetros.

        Parámetros:
          - params: diccionario con valores para la plantilla del prompt.

        Retorna:
          - Prompt generado.
        """
        if not isinstance(params, dict):
            raise TypeError("El parámetro 'params' debe ser un diccionario.")

        if not self.template:
            raise ValueError("No se ha cargado ninguna plantilla para generar el prompt.")

        return self.template.render(**params)
