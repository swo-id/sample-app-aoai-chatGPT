import os
import json
import logging
import requests
import dataclasses
import time

from typing import List

from backend.agent.model import Citation, Citations

DEBUG = os.environ.get("DEBUG", "false")
if DEBUG.lower() == "true":
    logging.basicConfig(level=logging.DEBUG)

AZURE_SEARCH_PERMITTED_GROUPS_COLUMN = os.environ.get(
    "AZURE_SEARCH_PERMITTED_GROUPS_COLUMN"
)

METADATA_TOOLS = [
    'get_list_documents_by_issue_year',
    'get_list_documents_by_expiration_year',
    'get_list_documents_already_expired',
    'get_list_all_documents_by_organization',
    'get_list_document_by_expiration_interval'
]

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


async def format_as_ndjson(r):
    try:
        async for event in r:
            yield json.dumps(event, cls=JSONEncoder) + "\n"
    except Exception as error:
        logging.exception("Exception while generating response stream: %s", error)
        yield json.dumps({"error": str(error)})


def parse_multi_columns(columns: str) -> list:
    if "|" in columns:
        return columns.split("|")
    else:
        return columns.split(",")


def fetchUserGroups(userToken, nextLink=None):
    # Recursively fetch group membership
    if nextLink:
        endpoint = nextLink
    else:
        endpoint = "https://graph.microsoft.com/v1.0/me/transitiveMemberOf?$select=id"

    headers = {"Authorization": "bearer " + userToken}
    try:
        r = requests.get(endpoint, headers=headers)
        if r.status_code != 200:
            logging.error(f"Error fetching user groups: {r.status_code} {r.text}")
            return []

        r = r.json()
        if "@odata.nextLink" in r:
            nextLinkData = fetchUserGroups(userToken, r["@odata.nextLink"])
            r["value"].extend(nextLinkData)

        return r["value"]
    except Exception as e:
        logging.error(f"Exception in fetchUserGroups: {e}")
        return []


def generateFilterString(userToken):
    # Get list of groups user is a member of
    userGroups = fetchUserGroups(userToken)

    # Construct filter string
    if not userGroups:
        logging.debug("No user groups found")

    group_ids = ", ".join([obj["id"] for obj in userGroups])
    return f"{AZURE_SEARCH_PERMITTED_GROUPS_COLUMN}/any(g:search.in(g, '{group_ids}'))"


def format_non_streaming_response(chatCompletion, history_metadata, apim_request_id):
    response_obj = {
        "id": chatCompletion.id,
        "model": chatCompletion.model,
        "created": chatCompletion.created,
        "object": chatCompletion.object,
        "choices": [{"messages": []}],
        "history_metadata": history_metadata,
        "apim-request-id": apim_request_id,
    }

    if len(chatCompletion.choices) > 0:
        message = chatCompletion.choices[0].message
        if message:
            if hasattr(message, "context"):
                response_obj["choices"][0]["messages"].append(
                    {
                        "role": "tool",
                        "content": json.dumps(message.context),
                    }
                )
            response_obj["choices"][0]["messages"].append(
                {
                    "role": "assistant",
                    "content": message.content,
                }
            )
            return response_obj

    return {}

def parse_citation(agent_response: dict) -> dict:
    '''
    parse citation from agent response if exist.
    Args:
        agent_response(dict): langchane agent responses
    Return:
        Citations: Citation format
    '''
    try:
        messages = agent_response.get('messages', [])
        citations = Citations(citations=[])

        if len(messages) == 0:
            return citations

        for messages_kind in messages:
            if messages_kind.type == 'tool' and messages_kind.name in METADATA_TOOLS:

                content = json.loads(messages_kind.content)
                if len(content) == 0:
                    continue

                for item in content:
                    citations.citations.append(Citation(
                        content=f'```json\n{json.dumps(item, indent=2, ensure_ascii=False)}\n',
                        title=item['documentTitle'],
                        url=None,
                        filepath=item['filepath']
                    ))

            elif messages_kind.type == 'tool' and \
                messages_kind.name == 'get_permit_document_content':

                content = json.loads(messages_kind.content)
                if len(content) == 0:
                    continue

                for item in content:
                    citations.citations.append(Citation(
                        content=item['content'],
                        title=item['title'],
                        url=None,
                        filepath=item['filepath'],
                        chunk_id=item['chunking_id']
                    ))

        return citations.model_dump()
    except Exception as e:
        print(f"Error parsing agent response: {e}")
        return Citations(citations=[]).model_dump()

def add_citation_markers_end(assistant_content, citations):
    """
    Add all citation markers at the end of the response
    """
    markers = ' '.join([f"[doc{i+1}]" for i in range(len(citations))])
    return f"{assistant_content} {markers}"

def format_non_streaming_responseV2(agent_response, history_metadata, apim_request_id):
    if agent_response:
        response_obj = {
            "id": agent_response['messages'][-1].id,
            "model": agent_response['messages'][-1].response_metadata['model_name'],
            "created": int(time.time()),
            "object": 'chat.completion',
            "choices": [{"messages": []}],
            "history_metadata": history_metadata,
            "apim-request-id": apim_request_id,
        }

        citations = parse_citation(agent_response)
        response_obj["choices"][0]["messages"].append(
                {
                    "role": "tool",
                    "content": json.dumps(citations),
                }
            )
        assistant_content = add_citation_markers_end(agent_response['messages'][-1].content, citations['citations'])
        response_obj["choices"][0]["messages"].append(
                {
                    "role": "assistant",
                    "content": assistant_content,
                }
            )

        return response_obj

    return {}

def format_stream_response(chatCompletionChunk, history_metadata, apim_request_id):
    response_obj = {
        "id": chatCompletionChunk.id,
        "model": chatCompletionChunk.model,
        "created": chatCompletionChunk.created,
        "object": chatCompletionChunk.object,
        "choices": [{"messages": []}],
        "history_metadata": history_metadata,
        "apim-request-id": apim_request_id,
    }

    if len(chatCompletionChunk.choices) > 0:
        delta = chatCompletionChunk.choices[0].delta
        if delta:
            if hasattr(delta, "context"):
                messageObj = {"role": "tool", "content": json.dumps(delta.context)}
                response_obj["choices"][0]["messages"].append(messageObj)
                return response_obj
            if delta.role == "assistant" and hasattr(delta, "context"):
                messageObj = {
                    "role": "assistant",
                    "context": delta.context,
                }
                response_obj["choices"][0]["messages"].append(messageObj)
                return response_obj
            if delta.tool_calls:
                messageObj = {
                    "role": "tool",
                    "tool_calls": {
                        "id": delta.tool_calls[0].id,
                        "function": {
                            "name" : delta.tool_calls[0].function.name,
                            "arguments": delta.tool_calls[0].function.arguments
                        },
                        "type": delta.tool_calls[0].type
                    }
                }
                if hasattr(delta, "context"):
                    messageObj["context"] = json.dumps(delta.context)
                response_obj["choices"][0]["messages"].append(messageObj)
                return response_obj
            else:
                if delta.content:
                    messageObj = {
                        "role": "assistant",
                        "content": delta.content,
                    }
                    response_obj["choices"][0]["messages"].append(messageObj)
                    return response_obj

    return {}


def format_pf_non_streaming_response(
    chatCompletion, history_metadata, response_field_name, citations_field_name, message_uuid=None
):
    if chatCompletion is None:
        logging.error(
            "chatCompletion object is None - Increase PROMPTFLOW_RESPONSE_TIMEOUT parameter"
        )
        return {
            "error": "No response received from promptflow endpoint increase PROMPTFLOW_RESPONSE_TIMEOUT parameter or check the promptflow endpoint."
        }
    if "error" in chatCompletion:
        logging.error(f"Error in promptflow response api: {chatCompletion['error']}")
        return {"error": chatCompletion["error"]}

    logging.debug(f"chatCompletion: {chatCompletion}")
    try:
        messages = []
        if response_field_name in chatCompletion:
            messages.append({
                "role": "assistant",
                "content": chatCompletion[response_field_name] 
            })
        if citations_field_name in chatCompletion:
            citation_content= {"citations": chatCompletion[citations_field_name]}
            messages.append({ 
                "role": "tool",
                "content": json.dumps(citation_content)
            })

        response_obj = {
            "id": chatCompletion["id"],
            "model": "",
            "created": "",
            "object": "",
            "history_metadata": history_metadata,
            "choices": [
                {
                    "messages": messages,
                }
            ]
        }
        return response_obj
    except Exception as e:
        logging.error(f"Exception in format_pf_non_streaming_response: {e}")
        return {}


def convert_to_pf_format(input_json, request_field_name, response_field_name):
    output_json = []
    logging.debug(f"Input json: {input_json}")
    # align the input json to the format expected by promptflow chat flow
    for message in input_json["messages"]:
        if message:
            if message["role"] == "user":
                new_obj = {
                    "inputs": {request_field_name: message["content"]},
                    "outputs": {response_field_name: ""},
                }
                output_json.append(new_obj)
            elif message["role"] == "assistant" and len(output_json) > 0:
                output_json[-1]["outputs"][response_field_name] = message["content"]
    logging.debug(f"PF formatted response: {output_json}")
    return output_json


def comma_separated_string_to_list(s: str) -> List[str]:
    '''
    Split comma-separated values into a list.
    '''
    return s.strip().replace(' ', '').split(',')


def clean_messages(messages: List)-> List:
    response: List = []

    if len(messages) == 0:
        return response

    for message in messages:
        if message == {} or message.get('content') == ''\
            or message.get("role") in ["tool", "function"]:
            continue
        response.append(message)

    return response

