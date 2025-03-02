from pprint import pprint
from typing import Literal, TypedDict, Dict
from langgraph.graph import StateGraph, START, END
from IPython.display import Image, display
import yaml

import snowflake.connector
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, concat, lit

from ia_service import BaseTextGenerator
from cortex_search import CortexSearchService

PATH_DOCUMENT_GENERATOR = 'ia_cortex\prompts\doc_generator.j2'
PATH_COCUMENT_EXAMPLE = 'docuemntation.txt'


class snowflake_session():
    def __init__(self, conn_name: str) -> None:
        account = "------------"
        user = "snowflake_coder_app"
        password = "---------"
        role = "accountadmin"
        warehouse = "compute_wh"
        database = "snowflake_coder"
        schema = "app"
        client_session_keep_alive = False

        connection_parameters = {
            "account": account,
            "user": user,
            "password": password,
            "role": role,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
            "client_session_keep_alive": client_session_keep_alive
        }

        self.session = Session.builder.configs(connection_parameters).create()

    def get_session(self):
        return self.session


class AppState(TypedDict):
    text_to_document: str
    count: int
    doc_generated: dict


def generate_documentation(state: AppState) -> str:
    session = snowflake_session("snowflake").get_session()
    cuestion_generator = BaseTextGenerator(
        session=session,
        prompt_template_file=PATH_DOCUMENT_GENERATOR
    )

    existing_documentation = session.table("snowflake_coder.app.snowflake_documentation")\
        .with_column("EXIST_DOCUMENTATION", concat(col("category"), lit(" - "), col("name")))\
        .select("EXIST_DOCUMENTATION")\
        .distinct()

    existing_docs = ''
    for i_doc in existing_documentation.to_local_iterator():
        existing_docs += i_doc['EXIST_DOCUMENTATION'] + "\n"

    params = {}

    params["text_to_document"] = state["text_to_document"]
    params["existing_docs"] = existing_docs

    doc_generated = cuestion_generator.generate_response(params)
    doc_generated = yaml.safe_load(doc_generated)
    doc_generated = {item["name"]:
                     {"category": item["category"],
                      "markdown": item["markdown"]} for item in doc_generated["generated_docs"]}

    return AppState(doc_generated=doc_generated)


def store_documentation(state: AppState) -> str:
    session = snowflake_session("snowflake").get_session()

    for name, doc in state["doc_generated"].items():
        doc_string = doc["markdown"]
        category = doc["category"]
        session.call("snowflake_coder.app.split_text_and_store", doc_string, category, name)

    return state


if __name__ == "__main__":

    workflow = StateGraph(AppState)
    workflow.add_node("generate_documentation", generate_documentation)
    workflow.add_node("store_documentation", store_documentation)

    workflow.add_edge(START, "generate_documentation")
    workflow.add_edge("generate_documentation", "store_documentation")
    workflow.add_edge("store_documentation", END)

    app = workflow.compile()
    mermaid_graph_code = app.get_graph().draw_mermaid()
    pprint(mermaid_graph_code, width=150)

    # read user_idea_example.txt
    with open(PATH_COCUMENT_EXAMPLE, "r") as file:
        text_to_document = file.read()

    state = AppState(text_to_document=text_to_document)
    result = app.invoke(state)
    pprint(result, width=150)
