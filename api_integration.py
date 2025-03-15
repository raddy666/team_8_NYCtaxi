import openai

# Set your OpenAI API key
openai.api_key = "Your Api Key"
def ask_chatgpt(prompt):
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    return response["choices"][0]["message"]["content"]

# Test the function
question = "What is the average wait time for a taxi in Queens at 7 PM?"
response = ask_chatgpt(question)
print(response)
