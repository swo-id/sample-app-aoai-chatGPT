""" Permit Metdata Class Implementation in Cosmos DB """
from datetime import datetime
from typing import Literal, Optional, Any, List
from azure.cosmos.aio import CosmosClient
from azure.cosmos import exceptions
from backend.settings import app_settings
from backend.aisearch.aisearch import title_search_client, retrieval_client

class CosmosPermitMetaData():
    """ Permit Metadata Cosmos DB realization """
    def __init__(self, cosmosdb_endpoint: str, credential: Any,
                 database_name: str, container_name: str
        ):
        self.cosmosdb_endpoint = cosmosdb_endpoint
        self.credential = credential
        self.database_name = database_name
        self.container_name = container_name

        try:
            self.cosmosdb_client = CosmosClient(self.cosmosdb_endpoint, credential=credential)
        except exceptions.CosmosHttpResponseError as e:
            if e.status_code == 401:
                raise ValueError("Invalid credentials") from e
            else:
                raise ValueError("Invalid CosmosDB endpoint") from e

        try:
            self.database_client = self.cosmosdb_client.get_database_client(database_name)
        except exceptions.CosmosResourceNotFoundError as e:
            raise ValueError("Invalid Permit CosmosDB database name") from e

        try:
            self.container_client = self.database_client.get_container_client(container_name)
        except exceptions.CosmosResourceNotFoundError as e:
            raise ValueError("Invalid Permit CosmosDB container name") from e
        
        self.main_organization = []

    async def ensure(self):
        if not self.cosmosdb_client or not self.database_client or not self.container_client:
            return False, "CosmosDB client not initialized correctly"
        try:
            database_info = await self.database_client.read()
        except:
            return False, f"CosmosDB database {self.database_name} on account {self.cosmosdb_endpoint} not found"
        
        try:
            container_info = await self.container_client.read()
        except:
            return False, f"CosmosDB container {self.container_name} not found"
            
        return True, "CosmosDB client initialized successfully"

    async def load_main_organizations(self):
        '''Load main organization list from distinct organization in the database'''
        query = """
                SELECT DISTINCT TOP 20 c.organization
                FROM c
                """

        iterators = self.container_client.query_items(
            query=query,
            # enable_cross_partition_query=True
        )

        items = []
        async for item in iterators:
            items.append(item['organization'])

        self.main_organization = items if items else []

    async def _ensure_main_organizations_loaded(self):
        '''Ensure main organizations are loaded before any operation'''
        if not self.main_organization:
            await self.load_main_organizations()

    async def get_list_documents_by_issue_year(
            self,
            permit_type: Literal['PLO', 'KKPR', 'KKPRL', 'Ijin Lingkungan'] | None = None,
            year: int | None = None,
            month: int | None = None,
            organization: Optional[str] = None,
            operator: Literal['equal', 'greater', 'less'] | None = None,
            order_by: Optional[Literal['latest', 'earliest']] = 'latest'
        ):
        """
        Get list of documents that issued in based on year and optionally filtered by permit type and organization.

        Args:
            year (int): Target document issued year.
            month (int, optional): Target document issued month.
            document_type (str, optional): Type of document to filter by PLO, KKPR, KKPRL, Ijin Lingkungan.
            operator (str, optional): Comparison operator ('equal', 'greater', 'less').
            order_by (str, optional): Order by 'latest' or 'earliest'.
            organization (str, optional): Organization to filter by.

        Example use cases:
            - Sebutkan PLO (CA TAHUN 2020) PMO PGN beserta Lokasinya : {"organization" : "PGN CA tahun 2020", "permit_type": "PLO", "year": 2020}
            - Sebutkan RU yang dokumen KKPRLnya di terbitkan pada tahun 2023! : {"organization" : "RU", "permit_type" : "KKPRL", "operator" : "equal", "year" : 2023}
            - Sebutkan Instalasi milik PGN yang memiliki KKPR dengan tanggal terbit paling lama ! : {"organization" : "PGN", "permit_type" : "KKPR", "order_by" : "earliest"}
            - Apa saja Instalasi DPPU yang dokumen KKPR nya di terbitkan pada tahun 2023 ? : {"organization" : "DPPU", "permit_type" : "KKPR", "operator" : "equal", "year" : 2023}
            - Sebutkan KKPR mana saja yang di terbitkan pada tahun 2024 pada cluster non PMO ! : {"organization" : "Non Cluster PMO", "permit_type" : "KKPR", "operator" : "equal", "year" : 2024}
            - Sebutkan nomor KKPRL milik SH PGN yang di terbitkan pada tahun 2023 ! : {"organization" : "PGN", "permit_type" : "KKPRL", "operator" : "equal", "year" : 2023}
            - Sebutkan instalasi milik SHU yang Persetujuan Lingkungannya diperbaharui paling baru ! : {'organization': 'SHU', 'permit_type': 'Ijin Lingkungan', 'order_by': 'latest'}
            - Sebutkan Persetujuan Lingkungan milik SH PGN yang terakhir kali di perbaharui ! : {'organization': 'PGN', 'permit_type': 'Ijin Lingkungan', 'order_by': 'latest'}
            - Kapan terakhir persetujuan lingkungan di EP Asset 1- Field Jambi diperbaharui ? : {'organization': 'Field Jambi', 'permit_type': 'Ijin Lingkungan', 'order_by': 'latest'}
            - Kapan saja dokumen UKL UPL di FT Bandung Group - Padalarang di perbaharui ? : {'organization': 'FT Bandung - Padalarang', 'permit_type': 'Ijin Lingkungan', 'order_by': 'latest'}
        """

        query = """
            SELECT c.documentTitle, c.permitType, c.organization, c.filepath,
                p.issueDate, p.permitSummary, p.permitNumber
            FROM c
            JOIN p IN c.permits
            WHERE p.issueDate != ""
        """

        conditions = []
        parameters: list[dict[str, object]] = []
        items = []

        if permit_type:
            conditions.append("c.permitType = @permitType")
            parameters.append({"name": "@permitType", "value": permit_type})

        if operator and year:
            if year is None:
                year = datetime.now().year

            parameters.append({"name": "@year", "value": year})

            if operator == 'greater':
                conditions.append("YEAR(p.issueDate) >= @year")
            elif operator == 'less':
                conditions.append("YEAR(p.issueDate) <= @year")
            else:  # equal
                conditions.append("YEAR(p.issueDate) = @year")

        if year and month:
            parameters.append(dict(name="@month", value=month))
            conditions.append("MONTH(p.issueDate) = @month")

        if organization:
            await self._ensure_main_organizations_loaded()
            if organization.strip() in self.main_organization:
                conditions.append("c.organization = @organization")
                parameters.append(dict(name="@organization", value=organization))
            else:
                conditions.append("RegexMatch(c.filepath, @organization, 'i')")
                parameters.append(dict(name="@organization", value=organization))

            query_with_condition = query + " AND " + " AND ".join(conditions)

            iterators = self.container_client.query_items(
                query=query_with_condition,
                parameters=parameters,
                # enable_cross_partition_query=True
            )

            # iterate results
            async for item in iterators:
                items.append(item)

            if not items:
                # Fallback to title search if no items found
                conditions = conditions[:-1]  # Remove last organization condition
                title_file_search = await title_search_client.full_text_search(
                    keyword=organization.strip(),
                    select_fields=["title", "titleWithExtension"],
                    search_fields=["title"],
                    top=10
                )

                list_of_titles = [doc['titleWithExtension'] for doc in title_file_search['value']]
                title_str = ",".join([f"'{t}'" for t in list_of_titles])
                conditions.append(f"c.documentTitle IN ({title_str})")

                query += " AND " + " AND ".join(conditions)

                iterators = self.container_client.query_items(
                    query=query,
                    parameters=parameters,
                    # enable_cross_partition_query=True
                )

                # iterate results
                async for item in iterators:
                    items.append(item)

                if not items:
                    return "No documents found issued in this year."

        doc_len = 20
        if order_by == 'latest':
            items.sort(key=lambda x: x.get('issueDate', ''), reverse=True)
        else:  # earliest
            items.sort(key=lambda x: x.get('issueDate', ''))

        filtered_items = items[:doc_len]

        result_list = [f"List of documents issued is {len(filtered_items)} items:"]
        for item in filtered_items:
            result_list.append(
            f"- {item['documentTitle']} - Permit Number: {item['permitNumber']} "
            f"\n  (Org: {item['organization']}, Issue Date: {item['issueDate']})"
            f"\n  Document path: {item.get('filepath', 'N/A')} "
            f"\n  Summary: {item['permitSummary']}"
        )

        return "\n".join(result_list)

    async def get_list_documents_by_expiration_year(
            self,
            permit_type: Literal['PLO'] = 'PLO',
            year: int | None = None,
            organization: Optional[str] = None,
            operator: Literal['equal', 'greater', 'less'] | None = None,
            order_by: Optional[Literal['latest', 'earliest']] = 'latest'
        ):
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
        parameters: list[dict[str, object]] = []

        if permit_type:
            conditions.append("c.permitType = @permitType")
            parameters.append({"name": "@permitType", "value": permit_type})

        if operator:
            if year is None:
                year = datetime.now().year
            parameters.append({"name": "@year", "value": year})

            if operator == 'greater':
                conditions.append("YEAR(p.expirationDate) >= @year")
            elif operator == 'less':
                conditions.append("YEAR(p.expirationDate) <= @year")
            else:  # equal
                conditions.append("YEAR(p.expirationDate) = @year")

        if organization:
            await self._ensure_main_organizations_loaded()
            if organization.strip() in self.main_organization:
                conditions.append("c.organization = @organization")
                parameters.append({"name": "@organization", "value": organization})
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

        results = self.container_client.query_items(
            query=query,
            parameters=parameters,
            # enable_cross_partition_query=True
        )

        # iterate results
        items = []
        async for item in results:
            items.append(item)

        if not items:
            return "No documents found expiring in this year."

        if order_by == 'latest':
            items.sort(key=lambda x: x.get('expirationDate', ''), reverse=True)
        else:  # earliest
            items.sort(key=lambda x: x.get('expirationDate', ''))

        result_list = []
        for item in items:
            result_list.append(
                f"- {item['documentTitle']} - Permit Number: {item['permitNumber']} "
                f"\n  (Org: {item['organization']}, Expires: {item['expirationDate']})"
                f"\n  Document path: {item.get('filepath', 'N/A')} "
                f"\n  Summary: {item['permitSummary']}"
            )

        return "\n".join(result_list)

    async def get_list_documents_already_expired(
            self,
            organization: Optional[str] = None,
            order_by: Optional[Literal['latest', 'earliest']] = 'latest'
    ):
        """
        Get list of documents that have already expired. This is only for PLO document types.

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
        parameters: list[dict[str, object]] = [{"name": "@currentDate", "value": current_date}]

        query = """
            SELECT c.documentTitle, c.permitType, c.organization, c.filepath,
                p.expirationDate, p.permitSummary, p.permitNumber, p.installation
            FROM c
            JOIN p in c.permits
            WHERE p.expirationDate < @currentDate AND
                c.permitType = 'PLO'
            """

        if organization:
            await self._ensure_main_organizations_loaded()
            if organization.strip() in self.main_organization:
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

        results = self.container_client.query_items(
            query=query,
            parameters=parameters,
            # enable_cross_partition_query=True
        )

        items = []
        async for item in results:
            items.append(item)

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

    async def get_list_document_by_expiration_interval(
            self,
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
            await self._ensure_main_organizations_loaded()
            if organization.strip() in self.main_organization:
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

        results = self.container_client.query_items(
            query=query,
            parameters=parameters
            # enable_cross_partition_query=True
        )

        items = []
        async for item in results:
            items.append(item)


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

    async def get_list_all_documents_by_organization(
            self,
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
            await self._ensure_main_organizations_loaded()
            if organization.strip() in self.main_organization:
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
            query += " WHERE " + " AND ".join(conditions)

        results = self.container_client.query_items(
            query=query,
            parameters=parameters,
            # enable_cross_partition_query=True
        )

        items = []
        async for item in results:
            items.append(item)

        doc_len = 30

        if not items:
            result_list =  ["No documents found for the specified organization."]
        else:
            filtered_items = items[:doc_len]
            result_list = [f"List of documents for organization {organization} items:"]
            if permit_type == 'PLO': # PLO has expiration date
                for item in filtered_items:
                    result_list.append(
                        f"- {item['documentTitle']} "
                        f"\n  - Permit Number: {item['permitNumber']} "
                        f"\n  - (Org: {item['organization']}, Issue Date: {item['issueDate']}, Expiration Date: {item['expirationDate']})"
                        f"\n  - Document path: {item.get('filepath', 'N/A')} "
                        f"\n  - Summary: {item['permitSummary']}"
                    )
            else:
                for item in items:
                    result_list.append(
                        f"- {item['documentTitle']} "
                        f"\n  - Permit Number: {item['permitNumber']} "
                        f"\n  - (Org: {item['organization']}, Issue Date: {item['issueDate']})"
                        f"\n  - Document path: {item.get('filepath', 'N/A')} "
                        f"\n  - Summary: {item['permitSummary']}"
                    )

        cosmos_result = "\n".join(result_list)

        final_result = f"========Metadata DB Results========:\n\n{cosmos_result}"

        return final_result

    async def get_non_empty_issue_date_document(
            self,
            organization: Optional[str] | None = None
    ) -> List[str]:
        """
        Get list of documents that have non-empty issueDate, optionally filtered by organization.

        Args:
            organization (str, optional): Organization to filter by.
        Return:
            list[str]: List of filenames.
        """

        conditions = []
        parameters = []
        filenames: List[str] = []

        query = """
                SELECT c.documentTitle, c.permitType, c.organization, c.filepath, p.issueDate
                FROM c
                JOIN p IN c.permits
                WHERE p.issueDate != ""
            """
        if organization:
            await self._ensure_main_organizations_loaded()
            if organization.strip() in self.main_organization:
                conditions.append("c.organization = @organization")
                parameters.append(dict(name="@organization", value=organization))
            else:
                conditions.append("RegexMatch(c.filepath, @organization, 'i')")
                parameters.append(dict(name="@organization", value=organization))

            query_with_condition = query + " AND " + " AND ".join(conditions)

            results = cosmos_client.container_client.query_items(
                query=query_with_condition,
                parameters=parameters,
                enable_cross_partition_query=True
            )

            items = []
            async for item in results:
                items.append(item)

            filenames = [item['filepath'] for item in items]

            if not items:
                # Fallback to title search if no items found
                conditions = conditions[:-1]  # Remove last organization condition
                title_file_search = await title_search_client.full_text_search(
                    keyword=organization.strip(),
                    select_fields=["title", "titleWithExtension"],
                    search_fields=["title"],
                    top=15
                )

                filenames = [doc['title'] for doc in title_file_search['value']]

        return filenames

cosmos_client = CosmosPermitMetaData(
    cosmosdb_endpoint=f"https://{app_settings.permit_metadata.account}.documents.azure.com:443/",
    credential=app_settings.permit_metadata.account_key,
    database_name=app_settings.permit_metadata.database,
    container_name=app_settings.permit_metadata.container
)
