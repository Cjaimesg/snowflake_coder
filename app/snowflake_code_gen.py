from snowflake.core import Root
from app.cortex_search_service import CortexSearchService


class SnowflakeCodeGenerator:
    def __init__(self, session, cortex_search_service: CortexSearchService, model: str = 'claude-3-5-sonnet'):
        """
        Inicializa el objeto SnowflakeCodeGenerator.

        Parámetros:
          - session: objeto de sesión de Snowflake.
          - cortex_search_service: instancia de CortexSearchService para generar el contexto.
          - model: nombre del modelo a utilizar en COMPLETE (puedes modificarlo según tus necesidades).
        """
        self.session = session
        self.cortex_search_service = cortex_search_service
        self.model = model  # Selecciona el modelo deseado

    def generate_code(self, user_request: str, filter_: dict, limit: int = 10):
        """
        Genera código SQL para Snowflake basado en la solicitud del usuario.

        Parámetros:
          - user_request: la cadena que contiene la solicitud del usuario.
          - filter_: diccionario de filtros para la búsqueda de contexto.
          - limit: número máximo de resultados a retornar para el contexto (por defecto 10).

        Retorna:
          - El código SQL generado por el modelo.
        """
        # Genera el contexto usando CortexSearchService
        context = self.cortex_search_service.generate_context(user_request, filter_, limit)

        # Configura el prompt para que el modelo genere código SQL válido para Snowflake.
        prompt = (
                "You are an expert engineer in Snowflake, with extensive experience in designing, implementing, and optimizing data solutions on Snowflake. "
                "You master SQL and Snowpark (Python) for creating stored procedures and always follow best practices in security, performance, and maintainability. "
                "When errors occur in the code, you must analyze the error message and adjust the code to resolve them in the next iteration.\n\n"
                "Additional Context:\n"
                f"{context}\n\n"
                "User Request:\n"
                f"{user_request}\n\n"
                "Generate SQL code that can be executed in Snowflake to fulfill the user's request. "
                "If stored procedures are required, you may use Snowpark (Python) as needed. "
                "Your response must contain only the code, without any additional explanations or extra comments, except those necessary for valid SQL syntax.\n\n"
                "Example of using Snowpark for stored procedures:\n"
                "```sql\n"
                "CREATE OR REPLACE PROCEDURE joblib_multiprocessing_proc(i INT)\n"
                "  RETURNS STRING\n"
                "  LANGUAGE PYTHON\n"
                "  RUNTIME_VERSION = 3.9\n"
                "  HANDLER = 'joblib_multiprocessing'\n"
                "  PACKAGES = ('snowflake-snowpark-python', 'joblib')\n"
                "AS $$\n"
                "import joblib\n"
                "from math import sqrt\n\n"
                "def joblib_multiprocessing(session, i):\n"
                "  result = joblib.Parallel(n_jobs=-1)(joblib.delayed(sqrt)(i ** 2) for i in range(10))\n"
                "  return str(result)\n"
                "$$;\n"
                "```\n"
                "<<< END OF PROMPT CONFIGURATION >>>"
            )
        # Llama al método que invoca la función COMPLETE en Snowflake
        sql_code = self.complete_text(prompt)
        sql_code = sql_code.replace("```sql", "").replace("```", "").strip()
        return sql_code

    def run_query(self, sql_code: str):
        """
        Ejecuta una consulta en Snowflake.

        Parámetros:
          - sql_code: la consulta SQL a ejecutar.
        """
        print('CODIGO SQL:', sql_code)  # Para depuración
        try:
            result = self.session.sql(sql_code).collect()[0]
            success = True
        except Exception as e:
            result = f"Error al ejecutar la consulta: {e}"
            success = False
        return result, success

    def format_prompt(self, prompt: str):
        """
        Realiza el formateo necesario del prompt para evitar errores con comillas.
        """
        return prompt.replace("'", "''")

    def split_sql(self, sql_code: str):
        """
        Divide el código SQL en instrucciones individuales.

        Parámetros:
          - sql_code: el código SQL a dividir.

        Retorna:
          - Una lista de instrucciones SQL individuales.
        """
        return sql_code.split(";")[:-1]

    def complete_text(self, prompt: str):
        """
        Llama a la función COMPLETE de Snowflake para generar el código SQL a partir del prompt.

        Parámetros:
          - prompt: la cadena que contiene el prompt completo (incluyendo contexto y solicitud).

        Retorna:
          - El código SQL generado por el modelo.
        """
        prompt = self.format_prompt(prompt)
        query = (
            f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{self.model}', '{prompt}') AS response"
        )

        print(query)  # Para depuración

        # Ejecuta la consulta en Snowflake y obtiene el resultado
        result = self.session.sql(query).collect()[0]['RESPONSE']

        return result
