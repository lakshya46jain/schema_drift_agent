import openai

openai.api_key = "sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

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
        response = openai.ChatCompletion.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=0.2,
        )
        return response.choices[0].message["content"]
    except Exception as e:
        return f"[Error calling LLM]: {str(e)}"