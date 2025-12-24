from typing import List, Dict, Any
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from backend.settings import app_settings

class AzureAISearch:

    """
    A class for searching Azure vector databases with configurable parameters.
    """

    def __init__(
            self,
            base_url: str,
            api_key: str,
            index_name: str,
            api_version: str = '2025-05-01-preview') -> None:
        """
        Initialize the VectorDatabaseSearcher.
        
        Args:
            base_url (str): Base URL for the Azure search endpoint.
            api_key (str): api key to access Azure AI Search.
            index_name (str): Name of the search index.
            api_version (str): API version to use. Defaults to "2025-09-01".
        """

        self.base_url = base_url
        self.api_key = api_key
        self.index_name = index_name
        self.api_version = api_version
        self.headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "api-key": f"{api_key}"
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError, ConnectionError))
    )
    async def _make_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Helper method to make asynchronous POST requests with retry."""
        url: str = f"{self.base_url}/indexes/{self.index_name}/docs/search?api-version={self.api_version}"

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=self.headers, json=payload) as response:
                if response.status != 200:
                    error_text = await response.text()
                    # Retry on transient errors: 429 (rate limit), 500, 502, 503, 504
                    if response.status in [429, 500, 502, 503, 504]:
                        raise TimeoutError(f"Azure AI Search transient error {response.status}: {error_text}")
                    raise Exception(f"Azure AI Search Error {response.status}: {error_text}")
                return await response.json()

    async def full_text_search(
            self,
            keyword: str,
            top: int = 5,
            select_fields: List[str] | None = None,
            search_fields: List[str] | None = None) -> Dict[str, Any]:
        """
        Search the Azure vector database using the provided vector.

        Args:
            vector (List[float]): The embedding vector to search with.
            k (int): Number of nearest neighbors to retrieve. Defaults to 5.
            select_fields : Fields to select in the response.
            vector_field (str): The vector field to search against.

        Returns:
            Dict[str, Any]: JSON response from the search API.
        """

        if select_fields is None:
            raise ValueError("select_fields must be provided")

        payload: Dict[str, Any] = {
            "search": keyword,
            "queryType": "full",
            "count": True,
            "top": top,
            "select": ", ".join(select_fields),
            "searchFields": ", ".join(search_fields) if search_fields else None,
        }

        return await self._make_request(payload)

    async def vector_search(
            self,
            vector: List[float],
            k: int = 5,
            select_fields: List[str] | None = None,
            vector_fields: List[str] = ["contentVector"]) -> Dict[str, Any]:
        """
        Search the Azure vector database using the provided vector.

        Args:
            vector (List[float]): The embedding vector to search with.
            k (int): Number of nearest neighbors to retrieve. Defaults to 5.
            select_fields : Fields to select in the response.
            vector_field (str): The vector field to search against. Defaults to "contentVector".

        Returns:
            Dict[str, Any]: JSON response from the search API.
        """

        if select_fields is None:
            raise ValueError("select_fields must be provided")

        payload: Dict[str, Any] = {
            "vectorQueries": [{
                "vector": vector,
                "fields": ", ".join(vector_fields),
                "k": k,
                "kind": "vector",
                "exhaustive": True
            }],
            "select": ", ".join(select_fields),
            "count": True
        }

        return await self._make_request(payload)

    async def hybrid_search(
            self,
            vector: List[float],
            keyword: str,
            k: int = 5,
            select_fields: List[str] | None = None,
            vector_fields: List[str] = ["contentVector"]) -> Dict[str, Any]:
        """
        Perform a hybrid search using both vector and keyword search.
        
        Args:
            vector (List[float]): The embedding vector to search with.
            keyword (str): The keyword to search with.
            k (int): Number of nearest neighbors to retrieve. Defaults to 5.
            select_fields : Fields to select in the response.
            vector_field (str): The vector field to search against. Defaults to "contentVector".
        """

        if select_fields is None:
            raise ValueError("select_fields must be provided")

        payload: Dict[str, Any] = {
            "vectorQueries": [{
                "vector": vector,
                "fields": ", ".join(vector_fields),
                "k": k,
                "kind": "vector"
            }],
            "search": keyword,
            "select": ", ".join(select_fields),
            "queryType": "semantic",
            "semanticConfiguration": app_settings.azure_aisearch.semantic_search_config,
            "top": str(k)
        }

        return await self._make_request(payload)

    async def semantic_ranking_search(
                                      self,
                                      keyword: str,
                                      k: int = 5,
                                      select_fields: List[str] | None = None
    ) -> Dict[str, Any]:
        """
        Perform a semantic ranking search using keyword search.
        
        Args:
            keyword (str): The keyword to search with.
            k (int): Number of nearest neighbors to retrieve. Defaults to 5.
            select_fields : Fields to select in the response.
        """

        if select_fields is None:
            raise ValueError("select_fields must be provided")

        payload: Dict[str, Any] = {
            "count": True,
            "vectorQueries": [
                {
                    "kind": app_settings.azure_aisearch.vectorizable_text_query_kind,
                    "text": keyword,
                    "fields": app_settings.azure_aisearch.vector_columns,
                    "queryRewrites": app_settings.azure_aisearch.query_rewrites,
                    "exhaustive": True,
                    "weight": 10,
                    "k": 5
                }
            ],
            "search": keyword,
            "queryType": "semantic",
            "captions": app_settings.azure_aisearch.query_captions,
            "answers": app_settings.azure_aisearch.query_answers,
            "semanticConfiguration": app_settings.azure_aisearch.semantic_search_config,
            "searchFields": app_settings.azure_aisearch.query_fields,
            "scoringProfile": app_settings.azure_aisearch.scoring_profile_config,
            "queryLanguage": app_settings.azure_aisearch.query_language,
            "select": ", ".join(select_fields),
            "queryRewrites": app_settings.azure_aisearch.query_rewrites,
            "debug": "queryRewrites"
        }

        return await self._make_request(payload)

retrieval_client = AzureAISearch(
    base_url=app_settings.azure_aisearch.endpoint or "",
    api_key=app_settings.azure_aisearch.key or "",
    index_name= app_settings.azure_aisearch.index
)

title_search_client = AzureAISearch(
    base_url=app_settings.azure_aisearch.endpoint or "",
    api_key=app_settings.azure_aisearch.key or "",
    index_name= app_settings.azure_aisearch.title_index
)
