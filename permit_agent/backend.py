import aiohttp
from typing import List, Dict, Any

class AzureAISearch:
    
    """
    A class for searching Azure vector databases with configurable parameters.
    """
    
    def __init__(self, base_url: str, api_key: str, index_name: str, api_version: str = '2025-05-01-preview') -> None:
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
    async def _make_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Helper method to make asynchronous POST requests."""
        url: str = f"{self.base_url}/indexes/{self.index_name}/docs/search?api-version={self.api_version}"
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=self.headers, json=payload) as response:
                response.raise_for_status()
                return await response.json()
    
    async def full_text_search(self, keyword: str, top: int = 5, select_fields: List[str] = None, search_fields: List[str] = None) -> Dict[str, Any]:
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
            "count": True,
            "top": top,
            "select": ", ".join(select_fields),
            "searchFields": ", ".join(search_fields),
        }

        return await self._make_request(payload)

    async def vector_search(self, vector: List[float], k: int = 5, select_fields: List[str] = None, vector_fields: List[str] = ["contentVector"]) -> Dict[str, Any]:
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

    async def hybrid_search(self, 
                      vector: List[float], 
                      keyword: str, 
                      k: int = 5, 
                      select_fields: List[str] = None, 
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
            "semanticConfiguration": "content-score",
            "top": str(k)
        }

        return await self._make_request(payload)

    async def semantic_ranking_search(
                                      self,
                                      keyword: str,
                                      k: int = 5,
                                      select_fields: List[str] = None
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
                    "kind": "text",
                    "text": keyword,
                    "fields": "titleVector,contentVector",
                    "queryRewrites": "generative",
                    "exhaustive": True,
                    "weight": 10,
                    "k": 5
                    }
                    ],
                    "search": keyword,
                    "queryType": "semantic",
                    "captions": "extractive",
                    "answers": "extractive|count-3",
                    "semanticConfiguration": "content-score",
                    "searchFields": "content, title",
                    "scoringProfile": "content-scoring",
                    "queryLanguage": "id-id",
                    "select": ", ".join(select_fields),
                    "queryRewrites": "generative",
                    "debug": "queryRewrites"
                    }

        return await self._make_request(payload)