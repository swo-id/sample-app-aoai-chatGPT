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

from azure.cosmos import CosmosClient

load_dotenv(".env", override=True)
print(f"Loaded environment: {os.getenv('ENV_NAME')}")

cosmos_client = CosmosClient(
    url=os.getenv("COSMOS_DB_URI"),
    credential=os.getenv("COSMOS_DB_KEY")
)

database_id = os.getenv("COSMOS_DB_DATABASE_ID")
container_id = os.getenv("COSMOS_DB_CONTAINER_ID")

database = cosmos_client.get_database_client(database_id)
container = database.get_container_client(container_id)

config_agent = AgentConfiguration()

EMBEDDINGS_MODEL = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME")
AZURE_OPENAI_SYSTEM_MESSAGE = config_agent.get_prompt(prompt_name='agent', version="0.2.1")
AZURE_AI_SEARCH_SCORING_PROFILE = os.getenv("AZURE_AI_SEARCH_SCORING_PROFILE")
AZURE_AI_SEARCH_SEMANTIC_CONFIGURATION = os.getenv("AZURE_AI_SEARCH_SEMANTIC_CONFIGURATION")

llm = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
    temperature=0,
    max_tokens=1000,
    timeout=500,
    max_retries=2
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
        scoring_profile=AZURE_AI_SEARCH_SCORING_PROFILE,
        semantic_configuration=AZURE_AI_SEARCH_SEMANTIC_CONFIGURATION
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
                permit_type: Literal['PLO'] = 'PLO'      
):
    """
    Get list of documents expiring in the next specified number of months, optionally filtered by organization.

    Args:
        months_ahead (int): Number of months ahead to check for expiration.
        organization (str, optional): Organization to filter by.
        permit_type (str, optional): Type of permit to filter by. PLO document types only.
    """

    query = """
        SELECT c.documentTitle, c.permitType, c.organization, c.filepath,
               p.issueDate, p.expirationDate, p.permitSummary, p.permitNumber, p.installation
        FROM c
        JOIN p in c.permits
        WHERE p.expirationDate >= GetCurrentDateTime()
              AND p.expirationDate <= DateTimeAdd("mm", @months_ahead, GetCurrentDateTime())
              AND c.permitType = @documentType
        """

    conditions = []
    parameters = []

    parameters.append(dict(name="@months_ahead", value=months_ahead))
    parameters.append(dict(name="@documentType", value=permit_type))

    if organization:
        if organization.strip() in MAIN_ORGANIZATIONS:
            conditions.append("c.organization = @organization")
            parameters.append(dict(name="@organization", value=organization))
        else:
            title_file_search = await title_search_client.full_text_search(
                keyword=organization.strip(),
                select_fields=["title", "titleWithExtension"],
                search_fields=["title"],
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
        return f"No documents found expiring in the next {months_ahead} months."

    result_list = []
    for item in items:
        result_list.append(
            f"- {item['documentTitle']} - {item['permitNumber']} "
            f"\n  (Org: {item['organization']}, Expires: {item['expirationDate']})"
            f"\n  Document path: {item.get('filepath', 'N/A')} "
            f"\n  Summary: {item['permitSummary']}"
        )

    return "\n".join(result_list)

@tool
async def get_list_documents_by_issue_year(
                permit_type: Literal['PLO', 'KKPR', 'KKPRL', 'Ijin Lingkungan'] = None,
                year: int = None,
                organization: Optional[str] = None, 
                operator: Literal['equal', 'greater', 'less'] = None,
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
    
    query = """
        SELECT c.documentTitle, c.permitType, c.organization, c.filepath,
               p.issueDate, p.permitSummary, p.permitNumber
        FROM c
        JOIN p IN c.permits
    """

    conditions = []
    parameters = []

    if permit_type:
        conditions.append("c.permitType = @permitType")
        parameters.append(dict(name="@permitType", value=permit_type))

    if operator and year:

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
            f"\n  (Org: {item['organization']}, Issue Date: {item['issueDate']})"
            f"\n  Document path: {item.get('filepath', 'N/A')} "
            f"\n  Summary: {item['permitSummary']}"
        )

    return "\n".join(result_list)

@tool
async def get_list_documents_by_expiration_year(
                permit_type: Literal['PLO'] = 'PLO',
                year: int = None,
                organization: Optional[str] = None, 
                operator: Literal['equal', 'greater', 'less'] = None,
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
    
    query = """
        SELECT c.documentTitle, c.permitType, c.organization, c.filepath,
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
            f"\n  (Org: {item['organization']}, Expires: {item['expirationDate']})"
            f"\n  Document path: {item.get('filepath', 'N/A')} "
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

    Args:
        organization (str, optional): Organization to filter by.
        order_by (str, optional): Order by 'latest' or 'earliest'.
    
    Example use cases:
      - Sebutkan dokumen yang sudah kadaluwarsa pada PGN SOR: {"organization" : "PGN SOR", "order_by" : "latest"}
      - Tampilkan dokumen PLO yang sudah kadaluwarsa pada KPI: {"organization" : "KPI", "order_by" : "latest"}
      - Sebutkan Instalasi mana saja yang PLO nya sudah kadaluwarsa di SH PPN !: {"organization" : "PPN", "order_by": "latest"}

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
            f"\n  (Org: {item['organization']}, Installation {item.get('installation', 'N/A')}, Expired: {item['expirationDate']})"
            f"\n  Document path: {item.get('filepath', 'N/A')} "
            f"\n  Summary: {item['permitSummary']}"
        )

    return "\n".join(result_list)

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

    conditions = []
    parameters = []

    query = """
        SELECT c.documentTitle, c.permitType, c.organization, c.filepath,
               p.issueDate, p.expirationDate, p.permitSummary, p.permitNumber
        FROM c
        JOIN p in c.permits
        """
    
    if permit_type:
        conditions.append("c.permitType = @permitType")
        parameters.append(dict(name="@permitType", value=permit_type))

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

    search_results = await retrieval_client.semantic_ranking_search(
        keyword=keyword,
        k=10, # number of top documents to retrieve
        select_fields=["title", "content"],
        scoring_profile=AZURE_AI_SEARCH_SCORING_PROFILE,
        semantic_configuration=AZURE_AI_SEARCH_SEMANTIC_CONFIGURATION
        # vector_fields=["contentVector"]
    )

    docs = [doc['content'] for doc in search_results['value']]
    title = [doc['title'] for doc in search_results['value']]

    if not items:
        return "No documents found for the specified organization."

    result_list = [f"List of documents for organization {organization} is {len(items)} items:"]
    for item in items:
        result_list.append(
            f"- {item['documentTitle']} - {item['permitNumber']} "
            f"\n  (Org: {item['organization']}, Issue Date: {item['issueDate']}, Expiration Date: {item['expirationDate']})"
            f"\n  Document path: {item.get('filepath', 'N/A')} "
            f"\nSummary: {item['permitSummary']}"
        )

    vector_result = "\n".join([f"{t}: {d}" for t, d in zip(title, docs)])
    cosmos_result = "\n".join(result_list)

    final_result = f"Content Search Results:\n{vector_result}\n\nMetadata DB Results:\n{cosmos_result}"

    return final_result


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