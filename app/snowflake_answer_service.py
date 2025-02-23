from snowflake.core import Root
from app.cortex_search_service import CortexSearchService


class SnowflakeAnswerService:
    def __init__(self, session, cortex_search_service: CortexSearchService, model: str = 'claude-3-5-sonnet'):
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
          "You are an assistant responsible for transforming a user's idea about implementing functionality in Snowflake into a YAML formatted list of detailed and sequential steps.\n\n"
          "Your task is to break down the user's idea, which might include several implicit components or steps, into a clear and comprehensive plan. Each step should cover a specific aspect of the development process in Snowflake, including:\n"
          "1. Problem definition and objective: Clearly state the goal and requirements behind the step.\n"
          "2. Design of the solution: Describe the architecture or logical approach, including details like object names, relationships, and overall structure.\n"
          "3. Configuration and setup: Include necessary environment configurations such as warehouses, roles, security settings, and database schemas. Each step should specify any prerequisite objects or configurations assumed to already exist.\n"
          "4. Code development: Provide detailed instructions for creating or modifying objects (tables, views, stored procedures, etc.).\n"
          "   - For stored procedures, do not include any parameters.\n"
          "5. Validation and testing: Specify how to test the step, including any unit tests or validation checks.\n"
          "Additionally, for each step, include a 'context' field that clearly states any prerequisite information, dependencies, or existing objects relevant to that step. This ensures that every instruction is provided with the full context required for its execution.\n\n"
          "The output must be strictly valid YAML without any additional text, commentary, or code outside the YAML structure.\n\n"
          "YAML Format to use:\n"
          "steps:\n"
          "  - step_name: <step_name>\n"
          "    step_type: <sql_code or documentation>\n"
          "    long_step_description: <detailed description of the step, including objectives, configurations, prerequisites, testing, and documentation recommendations>\n"
          "    objective: <objective of the step>\n"
          "    context: <any prerequisite information or existing objects relevant to this step>\n"
          "    object:\n"
          "      name: <name of the object to be created or modified>\n"
          "      type: <object type: table, view, stored procedure, etc.>\n"
          "  - step_name: <step_name>\n"
          "    long_step_description: <detailed description of the step>\n"
          "    objective: <objective of the step>\n"
          "    context: <any prerequisite information or existing objects relevant to this step>\n"
          "    object:\n"
          "      name: <object name>\n"
          "      type: <object type>\n\n"
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

        # Ejecuta la consulta a través de la sesión de Snowflake
        result = self.session.sql(query).collect()[0]['RESPONSE']

        # Se asume que la respuesta viene en el campo 'response'
        return result
