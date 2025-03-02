from pprint import pprint
from typing import Literal, TypedDict, Dict
from langgraph.graph import StateGraph, START, END
from IPython.display import Image, display
import yaml

import snowflake.connector
from snowflake.snowpark import Session

from ia_service import BaseTextGenerator
from cortex_search import CortexSearchService

PATH_CUESTION_GENERATOR = 'ia_cortex\prompts\question_generator.j2'
PATH_CUESTION_SUMARY = 'ia_cortex\prompts\question_sumary.j2'
# USER_IDEA_EXAMPLE = 'user_idea_example.txt'
USER_IDEA_EXAMPLE = 'user_idea_psw_pol.txt'


class snowflake_session():
    def __init__(self, conn_name: str) -> None:
        account = "----------------------"
        user = "snowflake_coder_app"
        password = "--------------"
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


# Definir los nodos
class AppState(TypedDict):
    user_idea: str
    count: int
    initials_questions: dict


def generate_question(state: AppState) -> str:
    session = snowflake_session("snowflake").get_session()
    cuestion_generator = BaseTextGenerator(
        session=session,
        prompt_template_file=PATH_CUESTION_GENERATOR
    )

    user_idea = state["user_idea"]

    initials_questions = cuestion_generator.generate_response({"user_idea": user_idea})
    initials_questions = yaml.safe_load(initials_questions)
    initials_questions = {item["title"]:
                          {"question": item["question"]} for item in initials_questions["initials_questions"]}

    return AppState(initials_questions=initials_questions)


def get_context_for_question(state: AppState) -> str:
    session = snowflake_session("snowflake").get_session()
    cortex_search_service = CortexSearchService(
        session=session,
        service_database="SNOWFLAKE_CODER",
        service_schema="APP",
        service_name="ADMIN_SNOWFLAKE_DOCUMENTATION_RAG",
        columns=[
            "PART",
            "CATEGORY",
            "NAME",
            "DEFINITION"
        ]
    )

    filter_ = {}

    initials_questions = {
        title: {
            **data,
            "answer_context": cortex_search_service.generate_context(data["question"], filter_, limit=5)
            }
        for title, data in state["initials_questions"].items()
        }

    return AppState(initials_questions=initials_questions)


def sumary_context_for_question(state: AppState) -> str:
    session = snowflake_session("snowflake").get_session()
    cuestion_generator = BaseTextGenerator(
        session=session,
        prompt_template_file=PATH_CUESTION_SUMARY
    )

    initials_questions = state["initials_questions"]

    answers = {
        title: {
            "question": data["question"],
            "summary": yaml.safe_load(cuestion_generator.generate_response(
                {"question": data["question"],
                 "context": "\n".join([f"{ctx['NAME']} - {ctx['PART']}\n{ctx['DEFINITION']}"
                                       for ctx in data["answer_context"]])
                    }
                ))
        }
        for title, data in initials_questions.items()
        }

    return AppState(initials_questions=answers)


if __name__ == "__main__":

    workflow = StateGraph(AppState)
    workflow.add_node("generate_question", generate_question)
    workflow.add_node("get_context_for_question", get_context_for_question)
    workflow.add_node("sumary_context_for_question", sumary_context_for_question)

    workflow.add_edge(START, "generate_question")
    workflow.add_edge("generate_question", "get_context_for_question")
    workflow.add_edge("get_context_for_question", "sumary_context_for_question")
    workflow.add_edge("sumary_context_for_question", END)

    app = workflow.compile()
    mermaid_graph_code = app.get_graph().draw_mermaid()
    pprint(mermaid_graph_code, width=150)

    # read user_idea_example.txt
    with open(USER_IDEA_EXAMPLE, "r") as file:
        user_idea = file.read()

    state = AppState(user_idea=user_idea)
    result = app.invoke(state)
    pprint(result, width=150)
