"""define model agent """
from typing import Optional
from pydantic import BaseModel, Field

class AgentResponse(BaseModel):
    """
    Response from the agent.
    """
    answer: str = Field(..., description="The answer to the user's question.")
    document: str = Field(..., description="The document citations used to answer the question.")

class Citation(BaseModel):
    """
    Citation for a document.
    """
    title: str = Field(..., description="The title of the document.")
    content: str = Field(...,
                         description="The content of the document or the summary for some tools")
    url: Optional[str] | None = Field(default=None, description="The URL of the document.")
    filepath: str = Field(..., description="The filepath of the document.")
    chunk_id: str = Field(default='0', description="The chunk ID of the document.")
    # default to 0 if not provided)

class Citations(BaseModel):
    """
    List of citations that found in tool response.
    """
    citations: list[Citation] = Field(..., description="The citations for the question.")
