import yaml

from typing import Literal, Optional

class AgentConfiguration:
    def __init__(self, file_path: str = 'config.yaml'):
        self.file_path = file_path
        with open(self.file_path, 'r') as file:
            self.config = yaml.safe_load(file)

    def get_prompt(self, prompt_name: str = Literal['agent'], version: Optional[str] = None):
        
        if version:
            filtered = list(filter(lambda x: x['version'] == version, self.config['prompt_config']['prompts'][prompt_name]))
            prompt = filtered[0]['prompt'] if filtered else None
        else:
            prompts = self.config['prompt_config']['prompts'][prompt_name]
            latest_prompt = max(prompts, key=lambda x: x['version'])
            prompt = latest_prompt['prompt']

        return prompt