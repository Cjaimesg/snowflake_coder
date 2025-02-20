import streamlit as st
import json

from app.session import snowflake_session
from app.cortex_search_service import CortexSearchService
from app.snowflake_answer_service import SnowflakeAnswerService


def main():
    st.title("Demo Cortex Search Service")
    st.write("Aplicación simple para realizar búsquedas usando my_cortex_service")

    if 'params_collected' not in st.session_state:
        st.session_state.params_collected = False

    # Init snowflake session
    sf_session = snowflake_session("snowflake")
    session = sf_session.get_session()

    # Configurar el servicio usando los mismos parámetros que en la creación
    service_database = "snowflake_coder"
    service_schema = "app"
    service_name = "my_cortex_service"
    cortex_service = CortexSearchService(session=session,
                                         service_database=service_database,
                                         service_schema=service_schema,
                                         service_name=service_name,
                                         col_context="agent_id",
                                         col_search="transcript_text")

    answer_service = SnowflakeAnswerService(session=session,
                                            cortex_search_service=cortex_service)

    # Entrada de usuario para la consulta de búsqueda
    query_input = st.text_input("Ingrese su búsqueda:", value="texto de búsqueda")

    # Filtro opcional: en este ejemplo se deja vacío, pero podrías agregar controles para definirlo
    filter_input = {}

    if st.button("Buscar"):
        try:
            rta = answer_service.generate_answer(query_input, filter_input, limit=5)
            st.write("Respuesta:")
            st.write(rta)

        except Exception as e:
            st.error(f"Error al realizar la búsqueda: {e}")



if __name__ == "__main__":
    main()

# conda activate snowflake_coder
# cd C:\Users\cajgo\Documents\snowflake_coder\snowflake_coder
# streamlit run snowflake_coder.py
