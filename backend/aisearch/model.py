'''  AI Search Response model definition '''
from pydantic import BaseModel, Field, field_validator

class Document(BaseModel):
    """
    Response from the AI Search.
    """
    id: str = Field(..., description="The unique ID of the document.")
    content: str = Field(..., description="The content of the AI Search response.")
    title: str = Field(..., description="The title of document from the AI Search response.")
    filepath: str = Field(...,description="The filepath of document from AI Search Response")
    chunking_id: str = Field(default="0", description="The unique ID of the document.",
                             alias="chunkingId")

    @field_validator('chunking_id', mode='before')
    @classmethod
    def convert_chunking_id(cls, v):
        return str(v) if v is not None else "0"
