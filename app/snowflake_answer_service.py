from snowflake.core import Root
from app.cortex_search_service import CortexSearchService


class SnowflakeAnswerService:
    def __init__(self, session, cortex_search_service: CortexSearchService, model: str = 'gemma-7b'):
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
            "Give me an answer in max 50 characters\n"  # <-- AQUI MODIFICAR: Indica instrucciones, estilo, etc.
            "Contexto:\n"
            f"{context}\n"
            "Pregunta del usuario:\n"
            f"{user_question}\n"
            "<<< FIN DE CONFIGURACION DEL PROMPT >>>"
        )

        # Llama a la función que realiza el completado de texto utilizando el modelo seleccionado
        answer = self.complete_text(prompt)
        return answer

    def complete_text(self, prompt: str):
        """
        Llama a la función COMPLETE de Snowflake para generar una respuesta a partir del prompt.

        Parámetros:
          - prompt: la cadena que contiene el prompt completo (incluyendo contexto y pregunta).

        Retorna:
          - La respuesta generada por el modelo.
        """
        # Opciones de configuración del modelo (puedes modificarlas según lo requieras)
        options = {
            'temperature': 0.7,
            'max_tokens': 4096
        }

        # Construye la consulta para llamar a la función COMPLETE.
        # Nota: Asegúrate de formatear correctamente la consulta para evitar problemas con comillas o caracteres especiales.
        query = (
            f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{self.model}', '{prompt}') AS response"
        )

        # Ejecuta la consulta a través de la sesión de Snowflake
        result = self.session.sql(query).collect()[0]['RESPONSE']

        # Se asume que la respuesta viene en el campo 'response'
        return result
