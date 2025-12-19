"""agent configurations"""
from typing import  Optional
import yaml

class AgentConfiguration:
    '''agent configuration class'''
    def __init__(self, file_path: str = 'prompt_config.yaml'):
        self.file_path = file_path
        with open(self.file_path, 'r', encoding='utf8') as file:
            self.config = yaml.safe_load(file)

    def get_prompt(self, prompt_name: str = 'agent', version: Optional[str] = None):
        '''Get prompt based on version choices'''
        if version:
            filtered = list(
                filter(
                    lambda x: x['version'] == version,
                    self.config['prompt_config']['prompts'][prompt_name]
                )
            )
            prompt = filtered[0]['prompt'] if filtered else None
        else:
            prompts = self.config['prompt_config']['prompts'][prompt_name]
            latest_prompt = max(prompts, key=lambda x: x['version'])
            prompt = latest_prompt['prompt']

        return prompt
