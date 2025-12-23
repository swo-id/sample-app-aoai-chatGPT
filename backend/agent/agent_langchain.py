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
AZURE_OPENAI_SYSTEM_MESSAGE = config_agent.get_prompt(prompt_name='agent', version='0.2.1')

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
        select_fields=["title", "content"]
        # vector_fields=["contentVector"]
    )

    docs = [doc['content'] for doc in search_results['value']]
    title = [doc['title'] for doc in search_results['value']]

    return "\n".join([f"{t}: {d}" for t, d in zip(title, docs)])

@tool
def get_current_year():
    """
    Get current year
    """

    return datetime.now().strftime("%Y")

@tool
def get_current_year_month():
    """
    Get current year and month in YYYY-MM format
    """

    return datetime.now().strftime("%Y-%m")

@tool
async def get_list_document_by_expiration_interval(
                months_ahead: int = 6,
                organization: Optional[str] = None,
                permit_type: Literal['PLO'] = 'PLO'):

    """
    Get list of documents expiring in the next specified number of months, optionally filtered by organization.

    Args:
        months_ahead (int): Number of months ahead to check for expiration.
        organization (str, optional): Organization to filter by.
        permit_type (str, optional): Type of permit to filter by. PLO document types only.
    """
    return await cosmos_client.get_list_document_by_expiration_interval(
        months_ahead=months_ahead,
        organization=organization,
        permit_type=permit_type
    )

@tool
async def get_list_documents_by_issue_year(
                permit_type: Literal['PLO', 'KKPR', 'KKPRL', 'Ijin Lingkungan'] = None, #type: ignore
                year: int = None, #type: ignore
                organization: Optional[str] = None,
                operator: Literal['equal', 'greater', 'less'] = None, #type: ignore
                order_by: Optional[Literal['latest', 'earliest']] = 'latest'):
    """
    Get list of documents that issued in based on year and optionally filtered by permit type and organization.
    
    Args:
        target_year (int): Target document issued year.
        document_type (str, optional): Type of document to filter by PLO, KKPR, KKPRL, Ijin Lingkungan.
        operator (str, optional): Comparison operator ('equal', 'greater', 'less').
        
    Example use cases:
        - Sebutkan PLO (CA TAHUN 2020) PMO PGN beserta Lokasinya : {"organization" : "PGN CA tahun 2020", "permit_type": "PLO", "year": 2020}
        - Sebutkan RU yang dokumen KKPRLnya di terbitkan pada tahun 2023! : {"organization" : "RU", "permit_type" : "KKPRL", "operator" : "equal", "year" : 2023}
        - Sebutkan Instalasi milik PGN yang memiliki KKPR dengan tanggal terbit paling lama ! : {"organization" : "PGN", "permit_type" : "KKPR", "order_by" : "earliest"}
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
                permit_type: Literal['PLO'] = 'PLO',
                year: int = None, #type: ignore
                organization: Optional[str] = None,
                operator: Literal['equal', 'greater', 'less'] = None, #type: ignore
                order_by: Optional[Literal['latest', 'earliest']] = 'latest'):
    """
    Get list of documents expiring in a specific year or organization.

    Args:
        year (int): Target document expiration year.
        permit_type (str, optional): Type of permit to filter by.
        organization (str, optional): Organization to filter by.
        operator (str, optional): Comparison operator ('equal', 'greater', 'less').
        order_by (str, optional): Order by 'latest' or 'earliest'.
    
    Example use cases:
        - Sebutkan area pada PGN SOR 1 yang paling cepat akan kadaluwarsa dan kapan kadaluwarsanya?: {"organization": "PGN SOR 1", "order_by": "earliest", "operator": "greater"}

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

    Args:
        organization (str, optional): Organization to filter by.
        order_by (str, optional): Order by 'latest' or 'earliest'.
    
    Example use cases:
      - Sebutkan dokumen yang sudah kadaluwarsa pada PGN SOR: {"organization" : "PGN SOR", "order_by" : "latest"}
      - Tampilkan dokumen PLO yang sudah kadaluwarsa pada KPI: {"organization" : "KPI", "order_by" : "latest"}
      - Sebutkan Instalasi mana saja yang PLO nya sudah kadaluwarsa di SH PPN !: {"organization" : "PPN", "order_by": "latest"}

    """
    return await cosmos_client.get_list_documents_already_expired(
        organization=organization,
        order_by=order_by
    )

@tool
async def get_list_all_documents_by_organization(
            organization: str,
            permit_type: Literal['PLO', 'KKPR', 'KKPRL', 'Ijin Lingkungan'],
            keyword: str
    ):
        """
        Get list of all documents by organization with no time filtering and ordering.

        Args:
            organization (str): Organization to filter by.
            permit_type (str): Type of permit to filter by PLO, KKPR, KKPRL, Ijin Lingkungan.
            keyword (str): Keyword to use for Azure AI Search to find relevant documents.         

        Example use cases:
            - Berapa jumlah dokumen PLO yang dimiliki oleh RU II: {"organization": "RU II", "permit_type": "PLO", "keyword": "PLO RU II"}
            - Sebutkan nomor SK Perstujuan Lingkungan yang ada di SOR 2 ! : {"organization": "SOR 2", "permit_type": "Ijin Lingkungan", "keyword": "SK Persetujuan Ijin Lingkungan SOR 2"}
            - Sebutkkan nomor KKPR yang dimiliki oleh IT Balongan ! : {"organization" : "IT Balongan", "permit_type": "KKPR", "keyword": "KKPR IT Balongan"}
        """
        return await cosmos_client.get_list_all_documents_by_organization(
            organization=organization,
            permit_type=permit_type,
            keyword=keyword
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
        get_current_year,
        get_current_year_month,
        get_list_documents_by_issue_year,
        get_list_documents_by_expiration_year,
        get_list_document_by_expiration_interval,
        get_list_documents_already_expired,
        get_list_all_documents_by_organization
    ]

agent = create_react_agent(llm, tools, prompt=AZURE_OPENAI_SYSTEM_MESSAGE)
