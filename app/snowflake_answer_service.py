from snowflake.core import Root
from app.cortex_search_service import CortexSearchService


class SnowflakeAnswerService:
    def __init__(self, session, cortex_search_service: CortexSearchService, model: str = 'llama3.1-70b'):
        """
        Inicializa el objeto SnowflakeAnswerService.

        Parámetros:
          - session: objeto de sesión de Snowflake.
          - cortex_search_service: instancia de CortexSearchService para generar el contexto.
          - model: nombre del modelo a utilizar en COMPLETE (puedes cambiarlo según tus necesidades).
        """
        self.session = session
        self.cortex_search_service = cortex_search_service
        self.model = model  # <-- AQUI PUEDES SELECCIONAR EL MODELO QUE DESEES

    def generate_answer(self, user_question: str, filter_: dict, limit: int = 10):
        """
        Genera una respuesta completa a partir de la pregunta del usuario.

        Parámetros:
          - user_question: la pregunta del usuario.
          - filter_: diccionario de filtros para la búsqueda de contexto.
          - limit: número máximo de resultados a retornar para el contexto (por defecto 10).

        Retorna:
          - La respuesta generada por el modelo de completado.
        """
        # Genera el contexto a partir del servicio Cortex Search
        context = self.cortex_search_service.generate_context(user_question, filter_, limit)

        # Configuración del prompt: Asegúrate de modificar esta plantilla según tus necesidades.
        prompt = (
          "You are an assistant in charge of transforming a user's idea regarding implementing functionality in Snowflake into a YAML formatted list of steps.\n\n"
          "Tasks:\n"
          "1. Analyze the user's idea, which may contain several implicit components or steps, and break it down into individual steps.\n"
          "2. Each step must clearly describe what is to be executed and must include the name of the object to be created.\n"
          "3. If a step involves creating a stored procedure, do not include any parameters.\n"
          "4. The output must be strictly valid YAML without any additional text, commentary, or code outside the YAML structure.\n\n"
          "YAML Format to use:\n"
          "steps:\n"
          "  - step_name: <step_name>\n"
          "    long_step_description: <step_description>\n"
          "    objetive: <objetive>\n"
          "    object:\n"
          "      name: <object_name>\n"
          "      type: <object_type>  # e.g., table, view, stored procedure, etc.\n"
          "  - step_name: <step_name>\n"
          "    long_step_description: <step_description>\n"
          "    objetive: <objetive>\n"
          "    object:\n"
          "      name: <object_name>\n"
          "      type: <object_type>\n\n"
          "Additional Context:\n"
          f"{context}\n\n"
          "User's Idea:\n"
          f"{user_question}\n\n"
          "<<< END OF PROMPT CONFIGURATION >>>"
          )

        # Llama a la función que realiza el completado de texto utilizando el modelo seleccionado
        answer = self.complete_text(prompt)
        return answer

    def format_prompt(self, prompt: str):
        prompt = prompt.replace("'", "''")
        return prompt

    def complete_text(self, prompt: str):
        """
        Llama a la función COMPLETE de Snowflake para generar una respuesta a partir del prompt.

        Parámetros:
          - prompt: la cadena que contiene el prompt completo (incluyendo contexto y pregunta).

        Retorna:
          - La respuesta generada por el modelo.
        """

        # Construye la consulta para llamar a la función COMPLETE.
        # Nota: Asegúrate de formatear correctamente la consulta para evitar problemas con comillas o caracteres especiales.

        prompt = self.format_prompt(prompt)
        query = (
            f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{self.model}', '{prompt}') AS response"
        )

        print(query)

        # Ejecuta la consulta a través de la sesión de Snowflake
        result = self.session.sql(query).collect()[0]['RESPONSE']

        # Se asume que la respuesta viene en el campo 'response'
        return result
