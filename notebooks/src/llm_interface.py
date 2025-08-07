import openai

def call_openai(prompt: str, model="gpt-4o", max_tokens=200) -> str:
    """
    Sends a prompt to OpenAI and returns the code suggestion.

    Parameters:
        prompt (str): The text prompt to send to the LLM.
        model (str): The OpenAI model to use (default: "gpt-4o").
        max_tokens (int): Max number of tokens in the completion.

    Returns:
        str: Model's response or error message.
    """
    try:
        client = openai.OpenAI(api_key = "sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        
        response = client.responses.create(
            model=model,
            input=prompt,
            max_output_tokens=max_tokens,
        )
        return response.output_text
    except Exception as e:
        return f"[Error calling LLM]: {str(e)}"