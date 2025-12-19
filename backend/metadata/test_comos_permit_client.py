import unittest

from cosmos_db_service import CosmosPermitMetaData


class TestCosmosPermitMetaData(unittest.TestCase):
    
    def setUp(self):
        self.client = CosmosPermitMetaData(
            cosmosdb_endpoint="https://test.documents.azure.com:443/",
            credential="test_key",
            database_name="test_db",
            container_name="test_container"
        )
    
    async def test_get_list_documents_by_issue_year(self):
        result = await self.client.get_list_documents_by_issue_year(
            permit_type="PLO",
            year=2023,
            operator="equal"
        )
        self.assertIsInstance(result, str)
    
    async def test_get_list_documents_by_expiration_year(self):
        result = await self.client.get_list_documents_by_expiration_year(
            permit_type="PLO",
            year=2024,
            operator="greater"
        )
        self.assertIsInstance(result, str)
    
    async def test_get_list_documents_already_expired(self):
        result = await self.client.get_list_documents_already_expired()
        self.assertIsInstance(result, str)


if __name__ == '__main__':
    unittest.main()