from snowflake.core import Root


# Clase que encapsula el servicio Cortex Search
class CortexSearchService:
    def __init__(self, session,
                 service_database: str,
                 service_schema: str,
                 service_name: str,
                 columns: list,
                 ):
        """
        Inicializa el objeto CortexSearchService.

        Parámetros:
          - session: objeto de sesión ya creado.
          - service_database: nombre de la base de datos donde reside el servicio.
          - service_schema: nombre del esquema donde reside el servicio.
          - service_name: nombre del servicio Cortex Search.
        """
        self.session = session
        self.service_database = service_database
        self.service_schema = service_schema
        self.service_name = service_name

        self.columns = columns

        # Crear el objeto root a partir de la sesión
        root = Root(self.session)
        # Obtener el objeto del servicio a partir de los parámetros proporcionados
        self.service = (
            root
            .databases[self.service_database]
            .schemas[self.service_schema]
            .cortex_search_services[self.service_name]
        )

    def search(self, query, filter_, limit=10):
        """
        Realiza una consulta al Cortex Search Service.

        Parámetros:
          - query: cadena de búsqueda.
          - filter_: diccionario con la definición del filtro.
          - limit: número máximo de resultados a retornar (por defecto 10).

        Retorna:
          - La respuesta de la consulta en formato JSON.
        """
        resp = self.service.search(
            query=query,
            columns=self.columns,
            filter=filter_,
            limit=limit
        )
        return resp

    def generate_context(self, query, filter_, limit=10):
        """
        Realiza una consulta al Cortex Search Service y genera un contexto.

        Parámetros:
          - query: cadena de búsqueda.
          - filter_: diccionario con la definición del filtro.
          - limit: número máximo de resultados a retornar (por defecto 10).

        Retorna:
          - El contexto generado.
        """

        results = self.search(query, filter_, limit).results

        return results
