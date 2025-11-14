import os

from datetime import datetime
from src.permit_agent.backend import AzureAISearch

from pydantic import BaseModel, Field

from langchain_core.tools import tool, Tool
from langchain_openai import AzureChatOpenAI
from langgraph.prebuilt import create_react_agent

from langfuse import get_client
from langfuse.langchain import CallbackHandler

from typing import Optional, Literal

from dotenv import load_dotenv

from openai import OpenAI

from azure.cosmos import CosmosClient

load_dotenv()

os.environ["LANGFUSE_SECRET_KEY"] = "sk-lf-a92829d1-413f-4994-af78-f3a336efce8a"
os.environ["LANGFUSE_PUBLIC_KEY"] = "pk-lf-60f090e3-f692-4515-8625-cd4b417e9d71"
os.environ["LANGFUSE_HOST"] = "http://localhost:3000"
 
langfuse = get_client()
langfuse_handler = CallbackHandler()

METADATA_DATABASE = "data/metadata_document.db"
cosmos_client = CosmosClient(
    url=os.getenv("COSMOS_DB_URI"),
    credential=os.getenv("COSMOS_DB_KEY")
)

database_id = "permitMetadataDB"
container_id = "permitMetadataContainer"

database = cosmos_client.get_database_client(database_id)
container = database.get_container_client(container_id)

 
try:
    langfuse.auth_check()
    print("Langfuse client is authenticated and ready!")
except Exception as e:
    print(f"Authentication failed: {e}")

EMBEDDINGS_MODEL = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME")
AZURE_OPENAI_SYSTEM_MESSAGE = """
        You are a knowledgeable assistant that answers questions using the context provided from PDF pages. Each page is represented by a chunk with a title (derived from the filename)
        When answering:
          - Use the provided context to support your response.
          - filter the context if document title is part of the questions and omit the main answer from another document title.
          - Always include inline citations that reference the source page by including the document title and chunkingId in square brackets (e.g., [DocumentName, Page 0]).
          - Only process the information by the same page first then continue to another page.
          - If the retrieved context does not fully answer the query, state that the answer is based on the available context and may be incomplete.
          - Answer in point form if the question asks for list of items.
          - You are provided with multiple tools to assist you in answering questions. Use that tools.

        Main Knowledge:
        - Organization that available: PPN, PGN, KPI, SHU

          
        Answer the user’s query as accurately as possible by the most relevance title while directly referencing the relevant pages.
          
          """

llm = AzureChatOpenAI(
    azure_deployment="gpt-4.1",
    api_version="2024-12-01-preview",
    temperature=0,
    max_tokens=1000,
    timeout=500,
    max_retries=2
)

client = OpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    base_url=f"{os.getenv('AZURE_OPENAI_ENDPOINT')}/openai/v1/",
)

retrieval_client = AzureAISearch(
    base_url=os.getenv("AZURE_AI_SEARCH_ENDPOINT"),
    api_key=os.getenv("AZURE_AI_SEARCH_API_KEY"),
    index_name=os.getenv("AZURE_AI_SEARCH_INDEX_NAME")
    )

cosmos_client = CosmosClient(
    url=os.getenv("COSMOS_DB_URI"),
    credential=os.getenv("COSMOS_DB_KEY")
)

database_id = "permitMetadataDB"
container_id = "permitMetadataContainer"

class AgentResponse(BaseModel):
    """
    Response from the agent.
    """
    answer: str = Field(..., description="The answer to the user's question.")
    document: str = Field(..., description="The document citations used to answer the question.")

async def get_permit_document_content(keyword: str):
    """
    Get relevant permit documents content relevant from Azure AI Search based on keyword.
    This used if the question requires to lookup into the documents and get relevant information.


    Args:
        keyword (str): The keyword to search for relevant documents or filename from previous file list search.
    Returns:
        str: The relevant documents concatenated as a single string.
    """

    # embeddings = client.embeddings.create(
    #     input=keyword,
    #     model=EMBEDDINGS_MODEL
    # )

    # vector = embeddings.data[0].embedding

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
def get_current_date():
    """
    Get current date
    """

    return datetime.now().strftime("%Y-%m-%d")


@tool
def get_time_difference(now_datetime: str, expired_datetime: str):
    """
    Calculate time difference between now and permit expiration

    Args:
        now_datetime (str): Current date with YYYY-mm-dd format.
        expired_datetime (str): Document expiration date with YYYY-mm-dd format.
    """

    now = datetime.strptime(now_datetime, "%Y-%m-%d")
    expired = datetime.strptime(expired_datetime, "%Y-%m-%d")
    delta = expired - now
    return delta.days

@tool
def get_list_documents_by_issue_year(
                permit_type: Literal['PLO', 'KKPR/KKPRL', 'Ijin Lingkungan'] = None,
                year: int = None,
                organization: Optional[str] = None, 
                operator: Literal['equal', 'greater', 'less'] = None,
                order_by: Optional[Literal['latest', 'earliest']] = 'latest'):
    """
    Get list of documents issued in a specific year.

    Args:
        target_year (int): Target document issued year.
        document_type (str, optional): Type of document to filter by.
        operator (str, optional): Comparison operator ('equal', 'greater', 'less').
    """
    
    query = """
        SELECT c.documentTitle, c.permitType, c.organization, 
               p.issueDate, p.permitSummary, p.permitNumber
        FROM c
        JOIN p IN c.permits
    """

    conditions = []
    parameters = []

    if permit_type:
        conditions.append("c.permitType = @permitType")
        parameters.append(dict(name="@permitType", value=permit_type))

    if operator:
        parameters.append(dict(name="@year", value=year))
        
        if operator == 'greater':
            conditions.append("YEAR(p.issueDate) >= @year")
        elif operator == 'less':
            conditions.append("YEAR(p.issueDate) <= @year")
        else:  # equal
            conditions.append("YEAR(p.issueDate) = @year")
        
    if organization:
        conditions.append("c.organization = @organization")
        parameters.append(dict(name="@organization", value=organization))

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    results = container.query_items(
        query=query,
        parameters=parameters,
        enable_cross_partition_query=True
    )

    items = [item for item in results]

    if not items:
        return "No documents found issued in this year."

    if order_by == 'latest':
        items.sort(key=lambda x: x.get('issueDate', ''), reverse=True)
    else:  # earliest
        items.sort(key=lambda x: x.get('issueDate', ''))

    result_list = [f"List of documents issued is {len(items)} items:"]
    for item in items:
        result_list.append(
            f"- {item['documentTitle']} - {item['permitNumber']} "
            f"(Org: {item['organization']}, Issue Date: {item['issueDate']})"
            f"\n  Summary: {item['permitSummary']}"
        )

    return "\n".join(result_list)

@tool
def get_list_documents_by_expiration_year(
                permit_type: Literal['PLO'] = None,
                year: int = None,
                organization: Optional[str] = None, 
                operator: Literal['equal', 'greater', 'less'] = None,
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
    
    query = """
        SELECT c.documentTitle, c.permitType, c.organization, 
               p.expirationDate, p.permitSummary, p.permitNumber
        FROM c
        JOIN p IN c.permits
    """

    conditions = []
    parameters = []

    if permit_type:
        conditions.append("c.permitType = @permitType")
        parameters.append(dict(name="@permitType", value=permit_type))

    if operator:
        parameters.append(dict(name="@year", value=year))
        
        if operator == 'greater':
            conditions.append("YEAR(p.expirationDate) >= @year")
        elif operator == 'less':
            conditions.append("YEAR(p.expirationDate) <= @year")
        else:  # equal
            conditions.append("YEAR(p.expirationDate) = @year")
    

    if organization:
        conditions.append("c.organization = @organization")
        parameters.append(dict(name="@organization", value=organization))

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    results = container.query_items(
        query=query,
        parameters=parameters,
        enable_cross_partition_query=True
    )

    items = [item for item in results]

    if not items:
        return "No documents found expiring in this year."

    if order_by == 'latest':
        items.sort(key=lambda x: x.get('expirationDate', ''), reverse=True)
    else:  # earliest
        items.sort(key=lambda x: x.get('expirationDate', ''))

    result_list = []
    for item in items:
        result_list.append(
            f"- {item['documentTitle']} - {item['permitNumber']} "
            f"(Org: {item['organization']}, Expires: {item['expirationDate']})"
            f"\n  Summary: {item['permitSummary']}"
        )

    return "\n".join(result_list)

@tool
def get_list_documents_already_expired(
                organization: Optional[str] = None,
                order_by: Optional[Literal['latest', 'earliest']] = 'latest'      
):
    """
    Get list of documents that have already expired.
    """

    current_date = datetime.now().strftime("%Y-%m-%d")
    conditions = []
    parameters = [dict(name="@currentDate", value=current_date)]

    query = """
        SELECT c.documentTitle, c.permitType, c.organization, c.filepath,
               p.expirationDate, p.permitSummary, p.permitNumber, p.installation
        FROM c
        JOIN p in c.permits
        WHERE p.expirationDate < @currentDate AND 
              c.permitType = 'PLO'
        """
    
    if organization:
        conditions.append("c.organization = @organization")
        parameters.append(dict(name="@organization", value=organization))

    if conditions:
        query += " AND " + " AND ".join(conditions)

    results = container.query_items(
        query=query,
        parameters=parameters,
        enable_cross_partition_query=True
    )

    items = [item for item in results]

    if not items:
        return "No documents have expired."
    
    if order_by == 'latest':
        items.sort(key=lambda x: x.get('expirationDate', ''), reverse=True)
    else:  # earliest
        items.sort(key=lambda x: x.get('expirationDate', ''))

    result_list = [f"Now is {current_date}. The following documents have already expired:"]
    for item in items:
        result_list.append(
            f"- {item['documentTitle']} - Permit Number: {item['permitNumber']} "
            f"(Org: {item['organization']}, Installation {item.get('installation', 'N/A')}, Expired: {item['expirationDate']})"
            f"\n  Summary: {item['permitSummary']}"
        )

    return "\n".join(result_list)

@tool
def get_list_all_documents_by_organization(
                organization: Optional[str] = None,
                permit_type: Optional[Literal['PLO', 'KKPR/KKPRL', 'Ijin Lingkungan']]= None      
):
    """
    Get list of all documents by organization.

    Args:
        organization (str, optional): Organization to filter by.
        permit_type (str, optional): Type of permit to filter by.
    """

    conditions = []
    parameters = []

    query = """
        SELECT c.documentTitle, c.permitType, c.organization, c.filepath,
               p.issueDate, p.expirationDate, p.permitSummary, p.permitNumber
        FROM c
        JOIN p in c.permits
        """
    
    if organization:
        conditions.append("c.organization = @organization")
        parameters.append(dict(name="@organization", value=organization))

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    results = container.query_items(
        query=query,
        parameters=parameters,
        enable_cross_partition_query=True
    )

    items = [item for item in results]

    if not items:
        return "No documents found for the specified organization."

    result_list = [f"List of documents for organization {organization} is {len(items)} items:"]
    for item in items:
        result_list.append(
            f"{item['documentTitle']} - {item['permitNumber']} "
            f"(Org: {item['organization']}, Issue Date: {item['issueDate']}, Expiration Date: {item['expirationDate']})"
            f"\nSummary: {item['permitSummary']}"
        )

    return "\n".join(result_list)


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