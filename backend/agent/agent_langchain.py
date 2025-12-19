'''agent'''
from datetime import datetime
from typing import Literal, Optional

from langchain_core.tools import tool, Tool
from langgraph.prebuilt import create_react_agent
from langchain_openai import AzureChatOpenAI
from pydantic import SecretStr

from backend.agent.config import AgentConfiguration
from backend.settings import app_settings
from backend.aisearch.aisearch import retrieval_client
from backend.metadata.cosmos_db_service import cosmos_client


config_agent = AgentConfiguration()

EMBEDDING_MODEL = app_settings.azure_openai.embedding_name
AZURE_OPENAI_SYSTEM_MESSAGE = config_agent.get_prompt(prompt_name='agent')

llm = AzureChatOpenAI(
    azure_deployment=app_settings.azure_openai.model,
    api_version=app_settings.azure_openai.preview_api_version,
    temperature=0,
    timeout=500,
    max_retries=2,
    api_key=SecretStr(app_settings.azure_openai.key) if app_settings.azure_openai.key else None
)

async def get_permit_document_content(keyword: str) -> str:
    """
    Get relevant permit documents content relevant from Azure AI Search based on keyword.
    This used if the question requires to lookup into the documents and get relevant information.


    Args:
        keyword (str): The keyword to search for relevant documents or filename from previous file list search.
    Returns:
        str: The relevant documents concatenated as a single string.
    """

    ## Change retrieval method and configuration as needed
    search_results = await retrieval_client.semantic_ranking_search(
        keyword=keyword,
        k=10, # number of top documents to retrieve
        select_fields=["title", "content"],
        # vector_fields=["contentVector"]
    )

    docs = [doc['content'] for doc in search_results['value']]
    title = [doc['title'] for doc in search_results['value']]

    return "\n".join([f"{t}: {d}" for t, d in zip(title, docs)])

@tool
def get_current_date() -> str:
    """
    Get current date
    """

    return datetime.now().strftime("%Y-%m-%d")

@tool
async def get_list_documents_by_issue_year(
                permit_type: Literal['PLO', 'KKPR', 'KKPRL', 'Ijin Lingkungan'] = None, #type: ignore
                year: int = None, #type: ignore
                organization: Optional[str] = None, 
                operator: Literal['equal', 'greater', 'less'] = None, #type: ignore
                order_by: Optional[Literal['latest', 'earliest']] = 'latest'):
    """
    Get list of documents issued in a specific year.

    Args:
        target_year (int): Target document issued year.
        document_type (str, optional): Type of document to filter by.
        operator (str, optional): Comparison operator ('equal', 'greater', 'less').
    """
    return await cosmos_client.get_list_documents_by_issue_year(
        permit_type=permit_type,
        year=year,
        organization=organization,
        operator=operator,
        order_by=order_by
    )

@tool
async def get_list_documents_by_expiration_year(
                permit_type: Literal['PLO'] = None, # type: ignore
                year: int = None, #type: ignore
                organization: Optional[str] = None,
                operator: Literal['equal', 'greater', 'less'] = None, #type: ignore
                order_by: Optional[Literal['latest', 'earliest']] = 'latest'):
    """
    Get list of documents expiring in a specific year.

    Args:
        year (int): Target document expiration year.
        permit_type (str, optional): Type of permit to filter by.
        organization (str, optional): Organization to filter by.
        operator (str, optional): Comparison operator ('equal', 'greater', 'less').
        order_by (str, optional): Order by 'latest' or 'earliest'.
    """

    return await cosmos_client.get_list_documents_by_expiration_year(
        permit_type=permit_type,
        year=year,
        organization=organization,
        operator=operator,
        order_by=order_by
    )

@tool
async def get_list_documents_already_expired(
                organization: Optional[str] = None,
                order_by: Optional[Literal['latest', 'earliest']] = 'latest'
):
    """
    Get list of documents that have already expired.
    """
    return await cosmos_client.get_list_documents_already_expired(
        organization=organization,
        order_by=order_by
    )

tools = [
    Tool(
        name="get_permit_document_content",
        description="""CRITICAL: Use this tool FIRST when the user asks ANY question about permits, permit content, or specific permit information.
        Input: A search query or relevant keywords from the question
        Returns: Top 10 relevant permit documents with titles and content that match the query
        Example use cases: 
        - "Berapa saja panjang submarine pipeline yang ada di IT semarang ?"
        - "Berapa kedalaman yang di tertera pada dokumen KKPRL untuk IT Jakarta ?"
        DO NOT try to answer questions about specific permits without calling this tool first.""",
        func=get_permit_document_content,
        coroutine=get_permit_document_content
        ),
    get_current_date,
    get_list_documents_by_issue_year,
    get_list_documents_by_expiration_year,
    get_list_documents_already_expired,
    # get_list_all_documents_by_organization
    ]

agent = create_react_agent(llm, tools, prompt=AZURE_OPENAI_SYSTEM_MESSAGE)
