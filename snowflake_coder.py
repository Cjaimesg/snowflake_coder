import streamlit as st
import yaml

from app.session import snowflake_session
from app.cortex_search_service import CortexSearchService
from app.snowflake_answer_service import SnowflakeAnswerService
from app.snowflake_code_gen import SnowflakeCodeGenerator
from utils.utils import generate_step_descriptions, get_step_type


def init_services():
    # Inicializa la sesión de Snowflake
    sf_session = snowflake_session("snowflake")
    session = sf_session.get_session()

    # Configuración de los parámetros del servicio
    service_database = "snowflake_coder"
    service_schema = "app"
    service_name = "my_cortex_service"

    cortex_service = CortexSearchService(
        session=session,
        service_database=service_database,
        service_schema=service_schema,
        service_name=service_name,
        col_context="agent_id",
        col_search="transcript_text"
    )

    answer_service = SnowflakeAnswerService(
        session=session,
        cortex_search_service=cortex_service
    )

    code_generator = SnowflakeCodeGenerator(
        session=session,
        cortex_search_service=cortex_service
    )

    return answer_service, code_generator


def process_query(query_input, answer_service, code_generator, execute_query):
    filter_input = {}  # Aquí puedes agregar controles para definir filtros
    # Generar la respuesta en YAML
    with st.spinner("Generando respuesta..."):
        rta = answer_service.generate_answer(query_input, filter_input, limit=5)
    st.subheader("Respuesta")
    # st.markdown(rta)

    # Validar y parsear YAML
    try:
        steps = yaml.safe_load(rta)
        st.success("El YAML es válido.")
    except yaml.YAMLError as e:
        st.error(f"Error al parsear el YAML: {e}")
        return

    code_generated_contex = 'Code previusly generated\n'
    steps_descriptions, step_types = generate_step_descriptions(steps), get_step_type(steps)

    for step in steps_descriptions:
        st.markdown(step)

    for idx, (step, step_type) in enumerate(zip(steps_descriptions, step_types), start=1):

        if step_type != "sql_code":
            continue

        st.code(f"**Paso {idx}:** {step} - Type: {step_type}")
        intentos = 0
        codigo_valido = False
        # Guardar el código generado en cada intento (para mostrarlo luego)
        sql_codes_intento = []

        # Variable para ir acumulando información de errores para pasar al generador
        error_context = ""
        code_generated_in_step = ""
        while intentos < 3 and not codigo_valido:
            intentos += 1
            st.markdown(f"**Paso {idx}:** {intentos}")
            with st.spinner(f"Generando código para el paso {idx} (intento {intentos})..."):
                # Se le pasa tanto el step, el código previo exitoso y los errores anteriores (si los hay)
                full_context = step + code_generated_contex + "\n" + code_generated_in_step + "\n" + error_context
                sql_codes_intento = code_generator.generate_code(full_context, filter_input, limit=5)
                sql_codes_intento = code_generator.split_sql(sql_codes_intento)

            todas_ejecutadas = True
            error_mensajes = []  # Acumulamos errores para retroalimentación
            error_context = ""
            codigo_valido = True

            for sql_code in sql_codes_intento:
                st.code(sql_code, language="sql")
                result, success = code_generator.run_query(sql_code)

                if success:
                    error_context += f"\nÉxito: {sql_code} \n{'*'*30}"
                else:
                    error_context += f"\nError: {sql_code} \n -> {result} \n{'*'*30}"
                    error_mensajes.append(result)
                    codigo_valido = False
                    todas_ejecutadas = False

            if codigo_valido:
                code_generated_contex += '\n' + ';'.join(sql_codes_intento) + '\n'
            else:
                code_generated_in_step = ';'.join(sql_codes_intento) + '\n'
                st.warning("Se intentará regenerar el código para corregir los errores.")
                st.code(error_context)

        if not codigo_valido:
            st.error(f"No se pudo ejecutar el código exitosamente para el paso {idx} después de 3 intentos.")


def main():
    st.title("Demo Snowflake Coder")
    st.write("Aplicación simple para generar pasos y código SQL basado en una idea.")

    # Inicializa servicios
    answer_service, code_generator = init_services()

    # Uso de un formulario para la entrada de la consulta
    with st.form("form_busqueda"):
        query_input = st.text_area("Ingrese su búsqueda o idea:",
                                   height=140)
        execute_query = st.checkbox("Ejecutar consultas SQL", value=True)
        submit = st.form_submit_button("Buscar")
    if submit:
        process_query(query_input, answer_service, code_generator, execute_query)


if __name__ == "__main__":
    main()


# conda activate snowflake_coder
# cd C:\Users\cajgo\Documents\snowflake_coder\snowflake_coder
# streamlit run snowflake_coder.py
