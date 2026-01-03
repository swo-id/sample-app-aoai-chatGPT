''' Cosmos DB Data models '''
import dataclasses
import json
import pydantic
from pydantic import Field

@dataclasses.dataclass
class PermitMeta:
    """ all of permits metadata should has PermitMeta attributes"""
    id: str
    filepath: str
    documentTitle: str
    organization: str
    keywords: list[str]
    permitType: str

    def to_dict(self):
        """convert to dict"""
        return dataclasses.asdict(self)

@dataclasses.dataclass
class PermitAttributes:
    """ common permit attributes except PLO """
    issueDate: str
    expirationDate: str
    permitSummary: str
    permitNumber: str #type: ignore

    def to_dict(self):
        """convert to dict"""
        return dataclasses.asdict(self)

    def to_json(self):
        """convert to json"""
        return json.dumps(self.to_dict())

@pydantic.dataclasses.dataclass
class PLOPermitAttributes(PermitAttributes):
    """PLO Permit Attributes need addition attribute """
    installation: str = Field(
        ..., description="The installation location associated with the PLO permit.")

    def to_dict(self):
        """convert to dict"""
        return dataclasses.asdict(self)

    def to_json(self):
        """ convert to json"""
        return json.dumps(self.to_dict())

@pydantic.dataclasses.dataclass
class PermitMetaDataInDB(PermitMeta):
    """Permit Metadata in DB"""
    permits: list[PermitAttributes] = Field(
        ..., description="The list of permits number associated with the permit document.")

    def to_dict(self):
        """convert to dict"""
        return dataclasses.asdict(self)

    def to_json(self):
        """ convert to json"""
        return json.dumps(self.to_dict())

@pydantic.dataclasses.dataclass
class PLOMetaDataInDB(PermitMeta):
    """PLO Permit Metadata in DB """
    permits: list[PLOPermitAttributes] = Field(
        ..., description="The list of permits number associated with the PLO permit document.")

    def to_dict(self):
        """convert to dict"""
        return dataclasses.asdict(self)

    def to_json(self):
        """ convert to json"""
        return json.dumps(self.to_dict())
