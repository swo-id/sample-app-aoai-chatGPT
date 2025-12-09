import os

from datetime import datetime
from src.permit_agent.backend import AzureAISearch
from src.utils.config import AgentConfiguration

from pydantic import BaseModel, Field

from langchain_core.tools import tool, Tool
from langchain_openai import AzureChatOpenAI
from langgraph.prebuilt import create_react_agent

from typing import Optional, Literal

from dotenv import load_dotenv

from openai import OpenAI

from azure.cosmos import CosmosClient

load_dotenv()

METADATA_DATABASE = "data/metadata_document.db"
cosmos_client = CosmosClient(
    url=os.getenv("COSMOS_DB_URI"),
    credential=os.getenv("COSMOS_DB_KEY")
)

database_id = "permitMetadataDB"
container_id = "permitMetadataContainer"

database = cosmos_client.get_database_client(database_id)
container = database.get_container_client(container_id)

config_agent = AgentConfiguration()

EMBEDDINGS_MODEL = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME")

AZURE_OPENAI_SYSTEM_MESSAGE = config_agent.get_prompt(prompt_name='agent')

llm = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
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

title_search_client = AzureAISearch(
    base_url=os.getenv("AZURE_AI_SEARCH_ENDPOINT"),
    index_name=os.getenv("AZURE_AI_SEARCH_TITLE_INDEX_NAME"),
    api_version=os.getenv("AZURE_AI_SEARCH_API_VERSION"),
    api_key=os.getenv("AZURE_AI_SEARCH_API_KEY") 
)

cosmos_client = CosmosClient(
    url=os.getenv("COSMOS_DB_URI"),
    credential=os.getenv("COSMOS_DB_KEY")
)

# Get main organizations list from distinct organizations in the database
query = """
        SELECT DISTINCT TOP 20 c.organization
        FROM c
        """
results = container.query_items(
    query=query,
    enable_cross_partition_query=True
)

MAIN_ORGANIZATIONS = [item['organization'] for item in results]

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
async def get_list_documents_by_issue_year(
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

        if year is None:
            year = datetime.now().year

        parameters.append(dict(name="@year", value=year))
        
        if operator == 'greater':
            conditions.append("YEAR(p.issueDate) >= @year")
        elif operator == 'less':
            conditions.append("YEAR(p.issueDate) <= @year")
        else:  # equal
            conditions.append("YEAR(p.issueDate) = @year")
        
    if organization:
        if organization.strip() in MAIN_ORGANIZATIONS:
            conditions.append("c.organization = @organization")
            parameters.append(dict(name="@organization", value=organization))
        else:
            title_file_search = await title_search_client.full_text_search(
                keyword=organization.strip(),
                select_fields=["title", "titleWithExtension"],
                search_fields=["title", "titleWithExtension"],
                top=10
            )

            list_of_titles = [doc['titleWithExtension'] for doc in title_file_search['value']]
            title_str = ",".join([f"'{t}'" for t in list_of_titles])
            conditions.append(f"c.documentTitle IN ({title_str})")


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
async def get_list_documents_by_expiration_year(
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
        if year is None:
            year = datetime.now().year
        parameters.append(dict(name="@year", value=year))
        
        if operator == 'greater':
            conditions.append("YEAR(p.expirationDate) >= @year")
        elif operator == 'less':
            conditions.append("YEAR(p.expirationDate) <= @year")
        else:  # equal
            conditions.append("YEAR(p.expirationDate) = @year")
    

    if organization:
        if organization.strip() in MAIN_ORGANIZATIONS:
            conditions.append("c.organization = @organization")
            parameters.append(dict(name="@organization", value=organization))
        else:
            title_file_search = await title_search_client.full_text_search(
                keyword=organization.strip(),
                select_fields=["title", "titleWithExtension"],
                search_fields=["title", "titleWithExtension"],
                top=10
            )

            list_of_titles = [doc['titleWithExtension'] for doc in title_file_search['value']]
            title_str = ",".join([f"'{t}'" for t in list_of_titles])
            conditions.append(f"c.documentTitle IN ({title_str})")

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
async def get_list_documents_already_expired(
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
        if organization.strip() in MAIN_ORGANIZATIONS:
            conditions.append("c.organization = @organization")
            parameters.append(dict(name="@organization", value=organization))
        else:
            title_file_search = await title_search_client.full_text_search(
                keyword=organization.strip(),
                select_fields=["title", "titleWithExtension"],
                search_fields=["title", "titleWithExtension"],
                top=10
            )

            list_of_titles = [doc['titleWithExtension'] for doc in title_file_search['value']]
            title_str = ",".join([f"'{t}'" for t in list_of_titles])
            conditions.append(f"c.documentTitle IN ({title_str})")

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
async def get_list_all_documents_by_organization(
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
        if organization.strip() in MAIN_ORGANIZATIONS:
            conditions.append("c.organization = @organization")
            parameters.append(dict(name="@organization", value=organization))
        else:
            title_file_search = await title_search_client.full_text_search(
                keyword=organization.strip(),
                select_fields=["title", "titleWithExtension"],
                search_fields=["title", "titleWithExtension"],
                top=10
            )

            list_of_titles = [doc['titleWithExtension'] for doc in title_file_search['value']]
            title_str = ",".join([f"'{t}'" for t in list_of_titles])
            conditions.append(f"c.documentTitle IN ({title_str})")

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